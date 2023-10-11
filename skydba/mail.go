package skydba

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"html/template"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/mail"
	"net/smtp"
	"path/filepath"
	"strings"

	"github.com/nkocsea/skylib_skylog/skylog"
)

type EmailSending struct {
	Id             int64
	CompanyId      int64
	BranchId       int64
	Type           int32
	Name           string
	Email          string
	Host           string
	Port           int32
	Protocol       string
	Auth           int32
	Password       string
	Signature      string
	AttachmentSize int64
	Disabled       int32
}

type EmailTemplate struct {
	Id          int64
	CompanyId   int64
	BranchId    int64
	Code        string
	Name        string
	Subject     string
	Content     string
	ContentType string
	Signature   string
	FromEmail   string
	Disabled    int32
	SenderInfo  EmailSending
}

type EmailMessage struct {
	Id         int64
	CompanyId  int64
	BranchId   int64
	Type       int32
	CheckupId  int64
	RegisterId int64
	Category   int32
	PartnerId  int64
	ToEmail    string
	CcEmail    string
	BccEmail   string
	Subject    string
	Content    string
	Signature  string
	FromEmail  string
	SenderId   int64
	SentDate   int64
	SentTimes  int32
	Status     int32
}

type SendMailData struct {
	CompanyId     int64
	BranchId      int64
	TemplateKey   string
	SenderEmail   string
	To            []mail.Address
	Cc            []mail.Address
	Bcc           []mail.Address
	SubjectParams map[string]interface{}
	BodyParams    map[string]interface{}
	Attachments   []string
}

type EmailContent struct {
	From        string
	To          string
	Cc          string
	Bcc         string
	Subject     string
	Body        string
	ContentType string
	Attachments map[string][]byte
}

func (content *EmailContent) AddAttachment(fullPath string) error {
	b, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return err
	}

	_, fileName := filepath.Split(fullPath)
	content.Attachments[fileName] = b
	return nil
}

func (content *EmailContent) ToBytes() []byte {
	buf := bytes.NewBuffer(nil)
	withAttachments := len(content.Attachments) > 0

	// build header
	buf.WriteString(fmt.Sprintf("Subject: %s\n", content.Subject))
	buf.WriteString(fmt.Sprintf("To: %s\n", content.To))
	if len(content.Cc) > 0 {
		buf.WriteString(fmt.Sprintf("Cc: %s\n", content.Cc))
	}

	if len(content.Bcc) > 0 {
		buf.WriteString(fmt.Sprintf("Bcc: %s\n", content.Bcc))
	}

	buf.WriteString("MIME-Version: 1.0\n")
	writer := multipart.NewWriter(buf)
	boundary := writer.Boundary()
	if withAttachments {
		buf.WriteString(fmt.Sprintf("Content-Type: multipart/mixed; boundary=%s\n", boundary))
		buf.WriteString(fmt.Sprintf("--%s\n", boundary))
	} else {
		buf.WriteString(fmt.Sprintf("Content-Type: %v; charset=utf-8\n", content.ContentType))
	}

	buf.WriteString(content.Body)
	if withAttachments {
		for k, v := range content.Attachments {
			buf.WriteString(fmt.Sprintf("\n\n--%s\n", boundary))
			buf.WriteString(fmt.Sprintf("Content-Type: %s\n", http.DetectContentType(v)))
			buf.WriteString("Content-Transfer-Encoding: base64\n")
			buf.WriteString(fmt.Sprintf("Content-Disposition: attachment; filename=%s\n", k))

			b := make([]byte, base64.StdEncoding.EncodedLen(len(v)))
			base64.StdEncoding.Encode(b, v)
			buf.Write(b)
			buf.WriteString(fmt.Sprintf("\n--%s", boundary))
		}

		buf.WriteString("--")
	}

	return buf.Bytes()
}

// GetEmailSenderInfo function return sender email info from database
func GetEmailSenderInfo(companyId int64, branchId int64, email string) (*EmailSending, error) {
	q := DefaultQuery()

	sql := `
		select t1.* 
		from email_sending t1 
		where t1.disabled = 0 
		and t1.deleted_at <= 0::bigint 
		and ((t1.company_id = $1 and t1.branch_id = $2) 
			or (t1.company_id = $1 and t1.branch_id = 0::bigint) 
			or (t1.company_id =0::bigint and t1.branch_id = 0::bigint)
		)
		and t1.email = $3
		order by t1.company_id desc, t1.branch_id desc
	`

	var items []*EmailSending
	if err := q.Query(sql, []interface{}{companyId, branchId, email}, &items); err != nil {
		return nil, err
	}

	if len(items) <= 0 {
		return nil, skylog.ReturnError(errors.New(fmt.Sprintf("Sender email %v not found", email)))
	}
	return items[0], nil
}

// GetEmailTemplateInfo function return email template info from database
func GetEmailTemplateInfo(companyId int64, branchId int64, templateKey string) (*EmailTemplate, error) {
	q := DefaultQuery()

	sql := `
		select t1.* 
		from email_template t1 
		where t1.disabled = 0 
		and t1.deleted_at <= 0::bigint 
		and ((t1.company_id = $1 and t1.branch_id = $2) 
			or (t1.company_id = $1 and t1.branch_id = 0::bigint) 
			or (t1.company_id =0::bigint and t1.branch_id = 0::bigint)
		)
		and t1.code  = $3
		order by t1.company_id desc, t1.branch_id desc 
	`

	var items []*EmailTemplate
	if err := q.Query(sql, []interface{}{companyId, branchId, templateKey}, &items); err != nil {
		return nil, err
	}

	if len(items) <= 0 {
		return nil, skylog.ReturnError(errors.New(fmt.Sprintf("Email template (key: %v) not found", templateKey)))
	}

	template := items[0]
	sender, err := GetEmailSenderInfo(companyId, branchId, template.FromEmail)
	if err != nil {
		return nil, err
	}
	template.SenderInfo = *sender

	return template, nil
}

func EmailOnly(emails []mail.Address) []string {
	returnVal := make([]string, len(emails))
	for idx, item := range emails {
		returnVal[idx] = item.Address
	}
	return returnVal
}

func EmailsToString(emails []mail.Address) string {
	if len(emails) > 0 {
		tmp := make([]string, len(emails))
		for idx, item := range emails {
			tmp[idx] = item.String()
		}
		return strings.Join(tmp, "; ")
	}
	return ""
}

func FormatTemplate(key string, content string, params map[string]interface{}) (string, error) {
	if len(params) > 0 {
		t := template.Must(template.New(key).Parse(content))
		buf := &bytes.Buffer{}
		if err := t.Execute(buf, params); err != nil {
			return "", err
		}
		return buf.String(), nil
	}
	return content, nil
}

func FormatSubject(templateId int64, subject string, params map[string]interface{}) (string, error) {
	return FormatTemplate(fmt.Sprintf("%v_subject", templateId), subject, params)
}

func FormatBody(templateId int64, body string, params map[string]interface{}) (string, error) {
	return FormatTemplate(fmt.Sprintf("%v_body", templateId), body, params)
}

func BuildEmailMessage(template EmailTemplate, content EmailContent) EmailMessage {
	now, _ := GetCurrentMillis()
	return EmailMessage{
		CompanyId: template.CompanyId,
		BranchId:  template.BranchId,
		ToEmail:   content.To,
		CcEmail:   content.Cc,
		BccEmail:  content.Bcc,
		Subject:   content.Subject,
		Content:   content.Body,
		FromEmail: content.From,
		SenderId:  template.SenderInfo.Id,
		SentDate:  now,
	}
}

func SendMail(ctx context.Context, data SendMailData) error {
	// validate data
	if len(data.To) <= 0 {
		return errors.New("To email cannot empty")
	}
	// load template
	template, err := GetEmailTemplateInfo(data.CompanyId, data.BranchId, data.TemplateKey)
	if err != nil {
		return err
	}

	// check sender email
	if data.SenderEmail != "" {
		sender, err := GetEmailSenderInfo(data.CompanyId, data.BranchId, data.SenderEmail)
		if err != nil {
			return err
		}
		template.SenderInfo = *sender
	}

	// build email content
	content := EmailContent{}

	// from email
	from := mail.Address{
		Name:    template.SenderInfo.Name,
		Address: template.SenderInfo.Email,
	}
	content.From = from.String()
	content.To = EmailsToString(data.To)
	content.Cc = EmailsToString(data.Cc)
	content.Bcc = EmailsToString(data.Bcc)

	// format subject
	subject, err := FormatSubject(template.Id, template.Subject, data.SubjectParams)
	if err != nil {
		return err
	}
	content.Subject = mime.QEncoding.Encode("UTF-8", subject)

	// format body
	body, err := FormatBody(template.Id, template.Content, data.BodyParams)
	if err != nil {
		return err
	}
	content.Body = body

	// content type
	contentType := strings.Trim(template.ContentType, " ")
	if contentType == "" {
		contentType = "text/html"
	}
	content.ContentType = contentType

	// build attachments
	if len(data.Attachments) > 0 {
		for _, item := range data.Attachments {
			content.AddAttachment(item)
		}
	}

	// Store message to db
	q := DefaultQuery()
	emailMessage := BuildEmailMessage(*template, content)
	q.Insert(ctx, &emailMessage)

	// mail server address
	smtpAddress := fmt.Sprintf("%v:%v", template.SenderInfo.Host, template.SenderInfo.Port)

	// init smtp auth
	var smtpAuth smtp.Auth
	if template.SenderInfo.Auth == 1 {
		smtpAuth = smtp.PlainAuth("", template.SenderInfo.Email, template.SenderInfo.Password, template.SenderInfo.Host)
	}
	// send mail
	err = smtp.SendMail(smtpAddress, smtpAuth, from.Address, EmailOnly(data.To), content.ToBytes())
	if err != nil {
		return err
	}
	// update send completed
	emailMessage.Status = 1 // Completed
	q.UpdateWithID(ctx, emailMessage)

	return nil
}
