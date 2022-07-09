package demo

type User struct {
	ID     string `json:"uid"`
	InTrip bool   `json:"in_trip"`
}

func NewUser(uid string) *User {
	user := User{
		ID:     uid,
		InTrip: false,
	}
	return &user
}
