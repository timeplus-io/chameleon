package handlers

import (
	"net/http"

	"github.com/timeplus-io/chameleon/generator/internal/job"
	"github.com/timeplus-io/chameleon/generator/internal/log"

	"github.com/gin-gonic/gin"
)

type JobHandler struct {
	manager *job.JobManager
}

type JobResponse struct {
	Id     string               `json:"id"`
	Name   string               `json:"name"`
	Status job.JobStatus        `json:"status"`
	Config job.JobConfiguration `json:"config"`
}

func NewJobHandler() *JobHandler {
	return &JobHandler{
		manager: job.NewJobManager(),
	}
}

// CreateJob godoc
// @Summary Create a job.
// @Description create a job.
// @Tags job
// @Accept json
// @Produce json
// @Param config body job.JobConfiguration true "job configuration"
// @Success 201 {object} JobResponse
// @Failure 400
// @Failure 500
// @Router /jobs [post]
func (h *JobHandler) CreateJob(c *gin.Context) {
	config := job.JobConfiguration{}

	if c.ShouldBind(&config) == nil {
		log.Logger().Infof("create job with config %v", config)

		if job, err := h.manager.CreateJob(config); err != nil {
			c.Status(http.StatusInternalServerError)
		} else {
			response := JobResponse{
				Id:     job.Id,
				Name:   job.Name,
				Status: job.Status,
				Config: job.Config,
			}
			c.JSON(http.StatusCreated, response)
		}
	} else {
		c.Status(http.StatusBadRequest)
	}
}

// ListJob godoc
// @Summary list all jobs.
// @Description list all jobs.
// @Tags job
// @Accept json
// @Produce json
// @Success 200 {array} job.Job
// @Router /jobs [get]
func (h *JobHandler) ListJob(c *gin.Context) {
	c.JSON(http.StatusOK, h.manager.ListJob())
}

// GetJob godoc
// @Summary get job by id.
// @Description get job by id.
// @Tags job
// @Accept json
// @Produce json
// @Param id path string true "job id"
// @Success 200 {object} job.Job
// @Failure 404
// @Router /jobs/{id} [get]
func (h *JobHandler) GetJob(c *gin.Context) {
	id := c.Param("id")
	if job, err := h.manager.GetJob(id); err != nil {
		c.Status(http.StatusNotFound)
	} else {
		// response := JobResponse{
		// 	Id:     job.Id,
		// 	Name:   job.Name,
		// 	Status: job.Status,
		// 	Config: job.Config,
		// }
		c.JSON(http.StatusOK, job)
	}
}

// DeleteJob godoc
// @Summary delete job by id.
// @Description delete job by id.
// @Tags job
// @Accept json
// @Produce json
// @Param id path string true "job id"
// @Success 204
// @Failure 404
// @Router /jobs/{id} [delete]
func (h *JobHandler) DeleteJob(c *gin.Context) {
	id := c.Param("id")
	if err := h.manager.DeleteJob(id); err != nil {
		c.Status(http.StatusNotFound)
	} else {
		c.Status(http.StatusNoContent)
	}
}

// StartJob godoc
// @Summary start to run a job.
// @Description start to run a job.
// @Tags job
// @Accept json
// @Produce json
// @Param id path string true "job id"
// @Success 204
// @Failure 404
// @Router /jobs/{id}/start [post]
func (h *JobHandler) StartJob(c *gin.Context) {
	id := c.Param("id")
	if err := h.manager.StartJob(id); err != nil {
		c.Status(http.StatusNotFound)
	} else {
		c.Status(http.StatusNoContent)
	}
}

// StopJob godoc
// @Summary stop a running job.
// @Description stop a running job.
// @Tags job
// @Accept json
// @Produce json
// @Param id path string true "job id"
// @Success 204
// @Failure 404
// @Router /jobs/{id}/stop [post]
func (h *JobHandler) StopJob(c *gin.Context) {
	id := c.Param("id")
	if err := h.manager.StopJob(id); err != nil {
		c.Status(http.StatusNotFound)
	} else {
		c.Status(http.StatusNoContent)
	}
}
