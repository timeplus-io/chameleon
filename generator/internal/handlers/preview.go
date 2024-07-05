package handlers

import (
	"net/http"

	fake "github.com/brianvoe/gofakeit/v6"
	"github.com/timeplus-io/chameleon/generator/internal/log"

	"github.com/gin-gonic/gin"
)

type PreviewRequest struct {
	Type string `json:"type"`
	Rule string `json:"rule"`
}

type PreviewResponse struct {
	Data string `json:"data"`
}

type PreviewHandler struct {
	faker *fake.Faker
}

func NewPreviewHandler() *PreviewHandler {
	return &PreviewHandler{
		faker: fake.New(0),
	}
}

func (h *PreviewHandler) makeGenerate(rule string) string {
	return h.faker.Generate(rule)
}

func (h *PreviewHandler) makeRegex(rule string) string {
	return h.faker.Regex(rule)
}

// Preview godoc
// @Summary Preview a generated data.
// @Description Preview a generated data.
// @Tags preview
// @Accept json
// @Produce json
// @Param config body PreviewRequest true "preview request"
// @Success 201 {object} PreviewResponse
// @Failure 400
// @Router /previews [post]
func (h *PreviewHandler) Preview(c *gin.Context) {
	req := PreviewRequest{}

	if c.ShouldBind(&req) == nil {
		log.Logger().Infof("preview %v", req)
		var generatedData string
		if req.Type == "generate" {
			generatedData = h.makeGenerate(req.Rule)
		} else if req.Type == "regex" {
			generatedData = h.makeRegex(req.Rule)
		} else {
			c.Status(http.StatusBadRequest)
		}

		response := PreviewResponse{
			Data: generatedData,
		}
		c.JSON(http.StatusCreated, response)

	} else {
		c.Status(http.StatusBadRequest)
	}
}
