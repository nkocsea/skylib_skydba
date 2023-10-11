package skydba

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/nkocsea/skylib_skylog/skylog"
	"github.com/nkocsea/skylib_skyutl/skyutl"
)

const (
	NOTIFY_IMAGE_URL  = "imageUrl"
	NOTIFY_ACTION_KEY = "actionKey"
)

type NotifyStatus int32

const (
	NotifyStatusNew NotifyStatus = iota
	NotifyStatusSending
	NotifyStatusSent
	NotifyStatusError
)

type NotificationMessage struct {
	Id              int64  `json:"id"`
	AccountId       int64  `json:"account_id"`
	Title           string `json:"title"`
	Body            string `json:"body"`
	ImageUrl        string `json:"image_url"`
	ActionKey       string `json:"action_key"`
	Data            string `json:"data"`
	SentAt          int64  `json:"sent_at"`
	Readed          int32  `json:"readed"`
	Status          int32  `json:"status"`
	StatusMessage   string `json:"status_message"`
	Document        string `json:"document"`
	SystemTableName string `protobuf:"bytes,9999,opt,name=system_table_name,json=system_table_name,table_name=notify_message,proto3" json:"system_table_name,omitempty"`
}

type NotificationMessageDevice struct {
	Id              int64  `json:"id"`
	MessageId       int64  `json:"message_id"`
	DeviceId        int64  `json:"device_id"`
	Status          int32  `json:"status"`
	StatusMessage   string `json:"status_message"`
	SystemTableName string `protobuf:"bytes,9999,opt,name=system_table_name,json=system_table_name,table_name=notify_message_device,proto3" json:"system_table_name,omitempty"`
}

var fcmClient *messaging.Client
var CoreServiceAddress string = ""
var isSendNotificationProcessing = false

func InitFCM(credentialJsonFilePath string) error {
	// Use the path to your service account credential json file
	opt := option.WithCredentialsFile(credentialJsonFilePath)
	// Create a new firebase app
	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		return err
	}
	// Get the FCM object
	fcmClient, err = app.Messaging(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func BuildMessage(title, body string, data map[string]string) NotificationMessage {
	returnVal := NotificationMessage{
		Title: title,
		Body:  body,
	}

	imageUrl, ok := data[NOTIFY_IMAGE_URL]
	if ok {
		returnVal.ImageUrl = imageUrl
		delete(data, NOTIFY_IMAGE_URL)
	}

	actionKey, ok := data[NOTIFY_ACTION_KEY]
	if ok {
		returnVal.ActionKey = actionKey
		delete(data, NOTIFY_ACTION_KEY)
	}

	if len(data) > 0 {
		jsonData, _ := json.Marshal(data)
		returnVal.Data = string(jsonData)
	}

	returnVal.Document = BuildDocument([]string{returnVal.Title, returnVal.Body})

	return returnVal
}

func BuildFCMMessage(data *NotificationMessage) (*messaging.Notification, *messaging.AndroidConfig, *messaging.WebpushConfig, *messaging.APNSConfig, *map[string]string) {
	// notification config
	notification := messaging.Notification{
		Title: data.Title,
		Body:  data.Body,
	}
	if len(data.ImageUrl) > 0 {
		notification.ImageURL = data.ImageUrl
	}

	// Android config
	var androidConfig messaging.AndroidConfig
	if len(data.ActionKey) > 0 || len(data.ImageUrl) > 0 {
		androidConfig = messaging.AndroidConfig{
			Notification: &messaging.AndroidNotification{
				ImageURL:    data.ImageUrl,
				ClickAction: data.ActionKey,
			},
		}
	}

	// Webpush config
	var webpushConfig messaging.WebpushConfig
	if len(data.ActionKey) > 0 || len(data.ImageUrl) > 0 {
		webpushConfig = messaging.WebpushConfig{}
		if len(data.ImageUrl) > 0 {
			webpushConfig.Headers = map[string]string{
				"image": data.ImageUrl,
			}
		}
		if len(data.ActionKey) > 0 {
			webpushConfig.FCMOptions = &messaging.WebpushFCMOptions{
				Link: data.ActionKey,
			}
		}
	}

	// Apns config
	var apnsConfig messaging.APNSConfig
	if len(data.ActionKey) > 0 || len(data.ImageUrl) > 0 {
		apnsConfig = messaging.APNSConfig{}
		if len(data.ImageUrl) > 0 {
			apnsConfig.Payload = &messaging.APNSPayload{
				Aps: &messaging.Aps{
					MutableContent: true,
				},
			}
			apnsConfig.FCMOptions = &messaging.APNSFCMOptions{
				ImageURL: data.ImageUrl,
			}
		}
		if len(data.ActionKey) > 0 {
			if apnsConfig.Payload != nil {
				if apnsConfig.Payload.Aps != nil {
					apnsConfig.Payload.Aps.Category = data.ActionKey
				} else {
					apnsConfig.Payload.Aps = &messaging.Aps{
						Category: data.ActionKey,
					}
				}
			} else {
				apnsConfig.Payload = &messaging.APNSPayload{
					Aps: &messaging.Aps{
						Category: data.ActionKey,
					},
				}
			}
		}
	}

	// data
	var notityData map[string]string
	if len(data.Data) > 0 {
		json.Unmarshal([]byte(data.Data), &notityData)
	}
	if notityData == nil {
		notityData = make(map[string]string)
	}
	notityData["notificationId"] = fmt.Sprintf("%v", data.Id)

	return &notification, &androidConfig, &webpushConfig, &apnsConfig, &notityData
}

func SendNotify(ctx context.Context, toAccountId []int64, title, body string) error {
	return SendNotifyWithData(ctx, toAccountId, title, body, nil)
}

func SendNotifyWithData(ctx context.Context, toAccountId []int64, title, body string, data map[string]string) error {
	// build message
	msg := BuildMessage(title, body, data)

	// get query
	q := DefaultQuery()
	// get transaction
	tx, err := DefaultBeginTx()
	if err != nil {
		return err
	}

	// create notification by account id
	for _, v := range toAccountId {
		saveMessage := NotificationMessage(msg)
		saveMessage.AccountId = v
		err := q.TxInsert(tx, ctx, &saveMessage)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	// commit transaction
	txErr := tx.Commit()
	if txErr != nil {
		tx.Rollback()
		return txErr
	}

	// call send notification
	err = skyutl.SendNotify(ctx, CoreServiceAddress)
	if err != nil {
		skylog.Error("SendNotifyWithData", err)
	}

	return nil
}

func UpdateStatusOfNotifyMessage(q *Q, tx *sql.Tx, id int64, status NotifyStatus, message string) error {
	_sql := `
		update notify_message
		set   status = $1
			, status_message = $2
			, sent_at = date_generator()
		where id = $3
	`

	var err error
	if tx != nil {
		_, err = q.TxExec(tx, _sql, int32(status), message, id)
	} else {
		_, err = q.Exec(_sql, int32(status), message, id)
	}

	return err
}

func UpdateStatusOfNotifyMessageDevice(q *Q, tx *sql.Tx, messageId int64, deviceId int64, status NotifyStatus, message string) error {
	_sql := `
		insert into notify_message_device(message_id, device_id, status, status_message)
		values ($1, $2, $3, $4)
	`

	var err error
	if tx != nil {
		_, err = q.TxExec(tx, _sql, messageId, deviceId, int32(status), message)
	} else {
		_, err = q.Exec(_sql, messageId, deviceId, int32(status), message)
	}

	return err
}

func FindNextNotification(q *Q, numOfMessagesWantSend int32) ([]*NotificationMessage, error) {
	// get next waiting message
	_sql := `
		select id
			, account_id
			, title
			, body
			, image_url
			, action_key
			, "data"
			, sent_at
			, readed
			, status
			, status_message
		from notify_message
		where status = $1
		and readed = $2
		order by created_at asc
		limit $3
	`

	if numOfMessagesWantSend < 1 {
		numOfMessagesWantSend = 1
	}

	var items []*NotificationMessage
	err := q.Query(_sql, []interface{}{int32(NotifyStatusNew), 0, numOfMessagesWantSend}, &items)
	if err != nil {
		return items, err
	}
	return items, nil
}

func DoSendNextNotification(numOfMessagesWantSend int32) error {
	// Check notification client is initialize
	if fcmClient == nil {
		return status.New(codes.Internal, "UNABLE_INITIALIZE_FIREBASE_CLIENT_ERR").Err()
	}

	// Check is process send notification is running
	if isSendNotificationProcessing {
		skylog.Info("DoSendNextNotification", "Stop because process is running...", time.Now())
		return nil
	}

	isSendNotificationProcessing = true
	startTime := time.Now()
	skylog.Info("DoSendNextNotification", startTime, "START")

	// get query
	q := DefaultQuery()
	items, err := FindNextNotification(q, numOfMessagesWantSend)
	if err != nil {
		isSendNotificationProcessing = false
		skylog.Info("DoSendNextNotification", startTime, "ERROR", err)
		return err
	}

	// check has waiting notification message
	if len(items) > 0 {
		for _, msg := range items {
			// find avaiable account
			account, err := GetAvailableAccountById(nil, msg.AccountId)
			if err != nil {
				UpdateStatusOfNotifyMessage(q, nil, msg.Id, NotifyStatusError, err.Error())
				continue
			}

			// check if account do not exists
			if account == nil || account.Id <= 0 {
				err = status.New(codes.Internal, "ACCOUNT_ID_NOT_FOUND_ERR").Err()
				UpdateStatusOfNotifyMessage(q, nil, msg.Id, NotifyStatusError, err.Error())
				continue
			} else {
				// find available device of user
				devices, err := GetAvailableDeviceByAccountId(nil, account.Id)
				if err != nil {
					UpdateStatusOfNotifyMessage(q, nil, msg.Id, NotifyStatusError, err.Error())
					continue
				}

				// check if account do not available devices
				if len(devices) == 0 {
					err = status.New(codes.Internal, "ACCOUNT_DO_NOT_AVAILABLE_DEVICE_ERR").Err()
					UpdateStatusOfNotifyMessage(q, nil, msg.Id, NotifyStatusError, err.Error())
					continue
				} else {
					// build FCM notification info
					notifyConfig, androidConfig, webpushConfig, apnsConfig, dataConfig := BuildFCMMessage(msg)

					// build token info
					tokenMap := make(map[string]int64)
					for _, v := range devices {
						tokenMap[v.Token] = v.Id
					}
					tokens := []string{}
					for k := range tokenMap {
						tokens = append(tokens, k)
					}

					errorList := []string{}
					// send to multi devices
					for _, v := range tokens {
						fcmMessage := messaging.Message{
							Data:         *dataConfig,
							Notification: notifyConfig,
							Android:      androidConfig,
							Webpush:      webpushConfig,
							APNS:         apnsConfig,
							Token:        v,
						}

						res, err := fcmClient.Send(context.Background(), &fcmMessage)
						if err != nil {
							errorList = append(errorList, err.Error())
							UpdateStatusOfNotifyMessageDevice(q, nil, msg.Id, tokenMap[v], NotifyStatusError, err.Error())
						} else {
							UpdateStatusOfNotifyMessageDevice(q, nil, msg.Id, tokenMap[v], NotifyStatusSent, res)
						}
					}

					if len(errorList) == len(tokens) {
						UpdateStatusOfNotifyMessage(q, nil, msg.Id, NotifyStatusError, strings.Join(errorList, " "))
					} else {
						UpdateStatusOfNotifyMessage(q, nil, msg.Id, NotifyStatusSent, strings.Join(errorList, " "))
					}
				}
			}
		}

		isSendNotificationProcessing = false
		skylog.Info("DoSendNextNotification", startTime, "END", err)

		skyutl.SendNotify(context.Background(), CoreServiceAddress)
	} else {
		isSendNotificationProcessing = false
		skylog.Info("DoSendNextNotification", startTime, "END", err)
	}

	return nil
}
