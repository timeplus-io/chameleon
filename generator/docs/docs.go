// Package docs Code generated by swaggo/swag. DO NOT EDIT
package docs

import "github.com/swaggo/swag"

const docTemplate = `{
    "schemes": {{ marshal .Schemes }},
    "swagger": "2.0",
    "info": {
        "description": "{{escape .Description}}",
        "title": "{{.Title}}",
        "contact": {
            "email": "gang@timeplus.io"
        },
        "version": "{{.Version}}"
    },
    "host": "{{.Host}}",
    "basePath": "{{.BasePath}}",
    "paths": {
        "/jobs": {
            "get": {
                "description": "list all jobs.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "job"
                ],
                "summary": "list all jobs.",
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "type": "array",
                            "items": {
                                "$ref": "#/definitions/job.Job"
                            }
                        }
                    }
                }
            },
            "post": {
                "description": "create a job.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "job"
                ],
                "summary": "Create a job.",
                "parameters": [
                    {
                        "description": "job configuration",
                        "name": "config",
                        "in": "body",
                        "required": true,
                        "schema": {
                            "$ref": "#/definitions/job.JobConfiguration"
                        }
                    }
                ],
                "responses": {
                    "201": {
                        "description": "Created",
                        "schema": {
                            "$ref": "#/definitions/handlers.JobResponse"
                        }
                    },
                    "400": {
                        "description": "Bad Request"
                    },
                    "500": {
                        "description": "Internal Server Error"
                    }
                }
            }
        },
        "/jobs/{id}": {
            "get": {
                "description": "get job by id.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "job"
                ],
                "summary": "get job by id.",
                "parameters": [
                    {
                        "type": "string",
                        "description": "job id",
                        "name": "id",
                        "in": "path",
                        "required": true
                    }
                ],
                "responses": {
                    "200": {
                        "description": "OK",
                        "schema": {
                            "$ref": "#/definitions/handlers.JobResponse"
                        }
                    },
                    "404": {
                        "description": "Not Found"
                    }
                }
            },
            "delete": {
                "description": "delete job by id.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "job"
                ],
                "summary": "delete job by id.",
                "parameters": [
                    {
                        "type": "string",
                        "description": "job id",
                        "name": "id",
                        "in": "path",
                        "required": true
                    }
                ],
                "responses": {
                    "204": {
                        "description": "No Content"
                    },
                    "404": {
                        "description": "Not Found"
                    }
                }
            }
        },
        "/jobs/{id}/start": {
            "post": {
                "description": "start to run a job.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "job"
                ],
                "summary": "start to run a job.",
                "parameters": [
                    {
                        "type": "string",
                        "description": "job id",
                        "name": "id",
                        "in": "path",
                        "required": true
                    }
                ],
                "responses": {
                    "204": {
                        "description": "No Content"
                    },
                    "404": {
                        "description": "Not Found"
                    }
                }
            }
        },
        "/jobs/{id}/stop": {
            "post": {
                "description": "stop a running job.",
                "consumes": [
                    "application/json"
                ],
                "produces": [
                    "application/json"
                ],
                "tags": [
                    "job"
                ],
                "summary": "stop a running job.",
                "parameters": [
                    {
                        "type": "string",
                        "description": "job id",
                        "name": "id",
                        "in": "path",
                        "required": true
                    }
                ],
                "responses": {
                    "204": {
                        "description": "No Content"
                    },
                    "404": {
                        "description": "Not Found"
                    }
                }
            }
        }
    },
    "definitions": {
        "handlers.JobResponse": {
            "type": "object",
            "properties": {
                "config": {
                    "$ref": "#/definitions/job.JobConfiguration"
                },
                "id": {
                    "type": "string"
                },
                "name": {
                    "type": "string"
                },
                "status": {
                    "$ref": "#/definitions/job.JobStatus"
                }
            }
        },
        "job.Job": {
            "type": "object",
            "properties": {
                "config": {
                    "$ref": "#/definitions/job.JobConfiguration"
                },
                "id": {
                    "type": "string"
                },
                "name": {
                    "type": "string"
                },
                "status": {
                    "$ref": "#/definitions/job.JobStatus"
                }
            }
        },
        "job.JobConfiguration": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                },
                "observer": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/observer.Configuration"
                    }
                },
                "sinks": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/sink.Configuration"
                    }
                },
                "source": {
                    "$ref": "#/definitions/source.Configuration"
                },
                "timeout": {
                    "type": "integer"
                }
            }
        },
        "job.JobStatus": {
            "type": "string",
            "enum": [
                "init",
                "running",
                "stopped",
                "failed"
            ],
            "x-enum-varnames": [
                "STATUS_INIT",
                "STATUS_RUNNING",
                "STATUS_STOPPED",
                "STATUS_FAILED"
            ]
        },
        "observer.Configuration": {
            "type": "object",
            "properties": {
                "properties": {
                    "type": "object",
                    "additionalProperties": true
                },
                "type": {
                    "type": "string"
                }
            }
        },
        "sink.Configuration": {
            "type": "object",
            "properties": {
                "properties": {
                    "type": "object",
                    "additionalProperties": true
                },
                "type": {
                    "type": "string"
                }
            }
        },
        "source.Configuration": {
            "type": "object",
            "properties": {
                "batch_number": {
                    "type": "integer"
                },
                "batch_size": {
                    "type": "integer"
                },
                "concurency": {
                    "type": "integer"
                },
                "fields": {
                    "type": "array",
                    "items": {
                        "$ref": "#/definitions/source.Field"
                    }
                },
                "interval": {
                    "type": "integer"
                },
                "interval_delta": {
                    "type": "integer"
                },
                "random_event": {
                    "type": "boolean"
                }
            }
        },
        "source.Field": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "array",
                    "items": {}
                },
                "name": {
                    "type": "string"
                },
                "range": {
                    "type": "array",
                    "items": {}
                },
                "rule": {
                    "type": "string"
                },
                "timestamp_delay_max": {
                    "type": "integer"
                },
                "timestamp_delay_min": {
                    "type": "integer"
                },
                "timestamp_format": {
                    "type": "string"
                },
                "timestamp_locale": {
                    "type": "string"
                },
                "type": {
                    "$ref": "#/definitions/source.FieldType"
                }
            }
        },
        "source.FieldType": {
            "type": "string",
            "enum": [
                "timestamp",
                "timestamp_int",
                "string",
                "int",
                "float",
                "bool",
                "map",
                "array",
                "generate",
                "regex"
            ],
            "x-enum-varnames": [
                "FIELDTYPE_TIMESTAMP",
                "FIELDTYPE_TIMESTAMP_INT",
                "FIELDTYPE_STRING",
                "FIELDTYPE_INT",
                "FIELDTYPE_FLOAT",
                "FIELDTYPE_BOOL",
                "FIELDTYPE_MAP",
                "FIELDTYPE_ARRAY",
                "FIELDTYPE_GENERATE",
                "FIELDTYPE_REGEX"
            ]
        }
    }
}`

// SwaggerInfo holds exported Swagger Info so clients can modify it
var SwaggerInfo = &swag.Spec{
	Version:          "1.0",
	Host:             "",
	BasePath:         "/api",
	Schemes:          []string{},
	Title:            "Chameleon Generator",
	Description:      "This is timeplus data generator api server.",
	InfoInstanceName: "swagger",
	SwaggerTemplate:  docTemplate,
	LeftDelim:        "{{",
	RightDelim:       "}}",
}

func init() {
	swag.Register(SwaggerInfo.InstanceName(), SwaggerInfo)
}
