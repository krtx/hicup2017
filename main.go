package main

import (
	"archive/zip"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
)

type usersType struct {
	Users []user `json:"users"`
}

type user struct {
	ID        int    `json:"id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Gender    string `json:"gender"`
	BirthDate int64  `json:"birth_date"`
}

var users map[int]user

type locationsType struct {
	Locations []location `json:"locations"`
}

type location struct {
	ID       int    `json:"id"`
	Place    string `json:"place"`
	Country  string `json:"country"`
	City     string `json:"city"`
	Distance int    `json:"distance"`
}

var locations map[int]location

type visitsType struct {
	Visits []visit `json:"visits"`
}

type visit struct {
	ID        int `json:"id"`
	Location  int `json:"location"`
	User      int `json:"user"`
	VisitedAt int `json:"visited_at"`
	Mark      int `json:"mark"`
}

var visits map[int]visit

func init() {
	users = make(map[int]user, 0)
	locations = make(map[int]location, 0)
	visits = make(map[int]visit, 0)
}

func loadData(archivePath string) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(rc)
		if err != nil {
			return err
		}

		switch {
		case strings.HasPrefix(f.Name, "users"):
			var data usersType
			if err = json.Unmarshal(b, &data); err != nil {
				return err
			}
			for _, user := range data.Users {
				users[user.ID] = user
			}
		case strings.HasPrefix(f.Name, "locations"):
			var data locationsType
			if err = json.Unmarshal(b, &data); err != nil {
				return err
			}
			for _, location := range data.Locations {
				locations[location.ID] = location
			}
		case strings.HasPrefix(f.Name, "visits"):
			var data visitsType
			if err = json.Unmarshal(b, &data); err != nil {
				return err
			}
			for _, visit := range data.Visits {
				visits[visit.ID] = visit
			}
		}
	}

	return nil
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := loadData(os.Getenv("ARCHIVE_PATH"))
	if err != nil {
		panic(err)
	}

	router := httprouter.New()

	router.GET("/users/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id, _ := strconv.Atoi(ps.ByName("id"))

		user, found := users[id]
		if !found {
			http.NotFound(w, r)
			return
		}

		b, _ := json.Marshal(user)
		w.Write(b)
	})

	router.GET("/locations/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id, _ := strconv.Atoi(ps.ByName("id"))

		location, found := locations[id]
		if !found {
			http.NotFound(w, r)
			return
		}

		b, _ := json.Marshal(location)
		w.Write(b)
	})

	router.GET("/visits/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id, _ := strconv.Atoi(ps.ByName("id"))

		visit, found := visits[id]
		if !found {
			http.NotFound(w, r)
			return
		}

		b, _ := json.Marshal(visit)
		w.Write(b)
	})

	router.GET("/users/:id/visits", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id, _ := strconv.Atoi(ps.ByName("id"))

		user, found := users[id]
		if !found {
			http.NotFound(w, r)
			return
		}

		q := r.URL.Query()

		var fromDate *int
		var toDate *int
		var country *string
		var toDistance *int

		if len(q["fromDate"]) > 0 {
			x, err := strconv.Atoi(q["fromDate"][0])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
			}
			return

			fromDate = &x
		}

		if len(q["toDate"]) > 0 {
			x, err := strconv.Atoi(q["toDate"][0])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
			}
			return

			toDate = &x
		}

		if len(q["country"]) > 0 {
			country = &q["country"][0]
		}

		if len(q["toDistance"]) > 0 {
			x, err := strconv.Atoi(q["toDistance"][0])
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
			}
			return

			toDistance = &x
		}

		resp := make([]visit, 0)
	})

	log.Fatal(http.ListenAndServe(":"+os.Getenv("PORT"), router))
}
