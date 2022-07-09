package common

import (
	"fmt"

	fake "github.com/brianvoe/gofakeit/v6"
)

type DimUser struct {
	ID         string `fake:"{u#####}"`
	FirstName  string `fake:"{firstname}"`
	LastName   string `fake:"{lastname}"`
	Email      string `fake:"{email}"`
	CreditCard string `fake:"{creditcardnumber}"`
	Gender     string `fake:"{regex:[FM]}"`
	Birthday   string `fake:"{byear}-{month}-{day}"`
}

func NewDimUser(id int) *DimUser {
	var user DimUser
	if err := fake.Struct(&user); err != nil {
		// this should never happen
		panic(fmt.Errorf("failed to generate DimUser data due to %w", err))
	}
	user.ID = fmt.Sprintf("u%05d", id)
	return &user
}

func (u *DimUser) ToEvent() map[string]any {
	result := make(map[string]any)
	result["uid"] = u.ID
	result["first_name"] = u.FirstName
	result["last_name"] = u.LastName
	result["email"] = u.Email
	result["credit_card"] = u.CreditCard
	result["gender"] = u.Gender
	result["birthday"] = u.Birthday
	return result
}

func (u *DimUser) ToRow() []any {
	return []any{u.ID, u.FirstName, u.LastName, u.Email, u.CreditCard, u.Gender, u.Birthday}
}

func GetDimUserHeader() []string {
	return []string{"uid", "first_name", "last_name", "email", "credit_card", "gender", "birthday"}
}

func MakeDimUsers(count int) []*DimUser {
	result := make([]*DimUser, count)
	for i := range result {
		result[i] = NewDimUser(i)
	}
	return result
}
