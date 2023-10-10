package skydba

import (
	"fmt"
	"time"
	// "github.com/tkanos/gonfig"
)

type AppConfig struct {
	ServiceName string `json:"serviceName"`
	Key         string `json:"key"`
	Value       string `json:"value"`
}

type ServiceAddrConfig struct {
	ServiceName string `json:"serviceName"`
	Host        string `json:"host"`
	Port        int32  `json:"port"`
}

type DbConfig struct {
	ServiceName string `json:"serviceName"`
	DbHost      string `json:"dbHost"`
	DbPort      int32  `json:"dbPort"`
	DbName      string `json:"dbName"`
	DbUser      string `json:"dbUser"`
	DbPassword  string `json:"dbPassword"`
	DbTimeOut   int32  `json:"dbTimeOut"`
	DbReconnect int32  `json:"dbReconnect"`
}

// Configuration struct: store all common config
type Configuration struct {
	NodeDbServer    string `json:"nodeDbServer"`
	NodeDbPort      int32  `json:"nodeDbPort"`
	NodeDbName      string `json:"nodeDbName"`
	NodeDbTimeOut   int32  `json:"nodeDbTimeOut"`
	NodeDbReconnect int32  `json:"nodeDbReconnect"`
	NodeDbUser      string `json:"nodeDbUser"`
	NodeDbPassword  string `json:"nodeDbPassword"`

	Debug                       bool          `json:"debug"`
	ServerPort                  int32         `json:"serverPort"`
	Authenticate                bool          `json:"authenticate"`
	DBServer                    string        `json:"dbServer"`
	DBPort                      int32         `json:"dbPort"`
	DBName                      string        `json:"dbName"`
	AppName                     string        `json:"appName"`
	DBTimeOut                   int32         `json:"dbTimeOut"`
	DBReconnect                 int32         `json:"dbReconnect"`
	DBUser                      string        `json:"dbUser"`
	DBPassword                  string        `json:"dbPassword"`
	PrivateKey                  string        `json:"privateKey"`
	JwtExpDuration              time.Duration `json:"jwtExpDuration"`
	CallTimeout                 int32         `json:"callTimeout"`
	ImageRatio                  float64       `json:"imageRatio"`
	HtmlToPdfApp                string        `json: "htmlToPdfApp"`
	ChunkSize                   int64         `json: "chunkSize"`
	AvatarBatchSize             int           `json: "avatarBatchSize"`
	ReportServer                string        `json:"ReportServer"`
	RestFileServerPort          int32         `json:"restFileServerPort"`
	PackagePrefix               string        `json:"packagePrefix"`
	DefaultDocumentCategoryName string        `json:"defaultDocumentCategoryName"`
	AllowGuestViewFile          bool          `json:"allowGuestViewFile"`
	NumOfNotifySendAtOneTime    int32         `json:"numOfNotifySendAtOneTime"`
}

// ServiceAddr struct
type ServiceAddr struct {
	CoreService string `json:"coreService"`
	File        string `json:"file"`
	Skydoc      string `json:"skydoc"`
	Report      string `json:"report"`
	Skyins      string `json:"skyins"`
	Skycmn      string `json:"skycmn"`
	Skyinv      string `json:"skyinv"`
	Skyatc      string `json:"skyatc"`
	Skyreg      string `json:"skyreg"`
	Skyimg      string `json:"skyimg"`
	Skyemr      string `json:"skyemr"`
	Skylab      string `json:"skylab"`
	Skyacc      string `json:"skyacc"`
	Skysle      string `json:"skysle"`
	Skyrpt      string `json:"skyrpt"`
	Skylis      string `json:"skylis"`
	Skyris      string `json:"skyris"`
	Skypacs     string `json:"skypacs"`
	Skysur      string `json:"skysur"`
}

func LoadServiceAddrConfig(q *Q, serviceName string, serviceAddr *ServiceAddr) error {
	var configs []ServiceAddrConfig
	_sql := `
		SELECT * 
		FROM service_addr_config
		WHERE disabled=0
	`
	param := []interface{}{}
	if err := q.Query(_sql, param, &configs); err != nil {
		return err
	}

	for _, c := range configs {
		switch c.ServiceName {
		case "coreService":
			serviceAddr.CoreService = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "file":
			serviceAddr.File = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skydoc":
			serviceAddr.Skydoc = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "report":
			serviceAddr.Report = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyins":
			serviceAddr.Skyins = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skycmn":
			serviceAddr.Skycmn = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyinv":
			serviceAddr.Skyinv = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyatc":
			serviceAddr.Skyatc = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyreg":
			serviceAddr.Skyreg = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyimg":
			serviceAddr.Skyimg = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyemr":
			serviceAddr.Skyemr = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyacc":
			serviceAddr.Skyacc = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skysle":
			serviceAddr.Skysle = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyrpt":
			serviceAddr.Skyrpt = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skylis":
			serviceAddr.Skylis = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skyris":
			serviceAddr.Skyris = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skylab":
			serviceAddr.Skylab = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skypacs":
			serviceAddr.Skypacs = fmt.Sprintf("%v:%v", c.Host, c.Port)
		case "skysur":
			serviceAddr.Skysur = fmt.Sprintf("%v:%v", c.Host, c.Port)
		}
	}

	return nil
}
