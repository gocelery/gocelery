package gocelery

type CeleryMessage struct {
	Properties
	Body string
}

type CeleryProperties struct {

	// task_id
	CorrelationID string `json:"correlation_id"`

	// application/json
	ContentType string `json:"content_type"`

	// utf-8
	ContentEncoding string `json:"content_encoding"`
	ReplyTo         string `json:"reply_to,omitempty"`
}

type CeleryHeader struct {
	ID       string
	RootID   string
	ParentID string
	GroupID  string `json:"group"`
	Lang     string // 'py'
	Task     string // 'proj.tasks.add'
	Method   string `json:"meth,omitempty"`
	Alias    string `json:"shadow,omitempty"`
	Retries  int    `json:"retries,omitempty"`
	Origin   string `json:"origin,omitempty"`
	//ArgRepresent
	//KwargRepresent
	// ETA
	// expires
	// timelimit (soft, hard)
}

/*
body = (
    object[] args,
    Mapping kwargs,
    Mapping embed {
        'callbacks': Signature[] callbacks,
        'errbacks': Signature[] errbacks,
        'chain': Signature[] chain,
        'chord': Signature chord_callback,
    }
)

W1s0LCA2XSwge30sIHsiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgImNoYWluIjogbnVsbCwgImNob3JkIjogbnVsbH1d
[
	[4, 6],
	{},
	{
		"callbacks": null,
		"errbacks": null,
		"chain": null,
		"chord": null
	}
]
*/
var body = 
	[]interface{}{
		[]interface{}{4, 6}, // args
		map[string]interface{}, // kwargs
		map[string]interface{}, // embed
	}
}

type Embed struct {
	CallBacks []Signature `json:"callbacks"`
	ErrBacks []Signature `json:"errbacks"`
	Chain []Signature `json:"chain"`
	Chord Signature `json:"chord"`
}

type Signature struct {

}

/*



"LPUSH" "celery" "{
	\"body\": \"W1s0LCA2XSwge30sIHsiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgImNoYWluIjogbnVsbCwgImNob3JkIjogbnVsbH1d\",
	\"content-encoding\": \"utf-8\",
	\"content-type\": \"application/json\",
	\"headers\": {
		\"lang\": \"py\",
		\"task\": \"tasks.add\",
		\"id\": \"e62be5e6-bfc1-441a-9295-c6ddda5e19f4\",
		\"shadow\": null,
		\"eta\": null,
		\"expires\": null,
		\"group\": null,
		\"retries\": 0,
		\"timelimit\": [null, null],
		\"root_id\": \"e62be5e6-bfc1-441a-9295-c6ddda5e19f4\",
		\"parent_id\": null,
		\"argsrepr\": \"(4, 6)\",
		\"kwargsrepr\": \"{}\",
		\"origin\": \"gen5321@Sicks-MacBook-Pro.local\"
	},
	\"properties\": {
		\"correlation_id\": \"e62be5e6-bfc1-441a-9295-c6ddda5e19f4\",
		\"reply_to\": \"9b5eb94f-ebd7-38f7-ac41-147d7ec57c81\",
		\"delivery_mode\": 2,
		\"delivery_info\": {
			\"exchange\": \"\",
			\"routing_key\": \"celery\"
		},
		\"priority\": 0,
		\"body_encoding\": \"base64\",
		\"delivery_tag\": \"d45f83b5-3b2a-49fb-ae28-1a42462fad1c\"
	}
}"
1533594724.330602 [0 127.0.0.1:55521] "HSET" "unacked" "d45f83b5-3b2a-49fb-ae28-1a42462fad1c" "[
	{
		\"body\": \"W1s0LCA2XSwge30sIHsiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgImNoYWluIjogbnVsbCwgImNob3JkIjogbnVsbH1d\",
		\"content-encoding\": \"utf-8\",
		\"content-type\": \"application/json\",
		\"headers\": {
			\"lang\": \"py\",
			\"task\": \"tasks.add\",
			\"id\": \"e62be5e6-bfc1-441a-9295-c6ddda5e19f4\",
			\"shadow\": null,
			\"eta\": null,
			\"expires\": null,
			roup\": null,
			\"retries\": 0,
			\"timelimit\": [null, null],
			\"root_id\": \"e62be5e6-bfc1-441a-9295-c6ddda5e19f4\",
			\"parent_id\": null,
			\"argsrepr\": \"(4, 6)\",
			\"kwargsrepr\": \"{}\",
			\"origin\": \"gen5321@Sicks-MacBook-Pro.local\"
		},
		\"properties\": {
			\"correlation_id\": \"e62be5e6-bfc1-441a-9295-c6ddda5e19f4\",
			\"reply_to\": \"9b5eb94f-ebd7-38f7-ac41-147d7ec57c81\",
			\"delivery_mode\": 2,
			\"delivery_info\": {
				\"exchange\": \"\",
				\"routing_key\": \"celery\"
			},
			\"priority\": 0,
			\"body_encoding\": \"base64\",
			\"delivery_tag\": \"d45f83b5-3b2a-49fb-ae28-1a42462fad1c\"
		}
	},
	\"\",
	\"celery\"
]"
*/
