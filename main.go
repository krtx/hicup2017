package main

import (
	"archive/zip"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
	_ "github.com/mattn/go-sqlite3"
)

// type usersType struct {
// 	Users []user `json:"users"`
// }

type user struct {
	ID        int    `json:"id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Gender    string `json:"gender"`
	BirthDate int64  `json:"birth_date"`
}

// var users map[int]user

// type locationsType struct {
// 	Locations []location `json:"locations"`
// }

type location struct {
	ID       int    `json:"id"`
	Place    string `json:"place"`
	Country  string `json:"country"`
	City     string `json:"city"`
	Distance int    `json:"distance"`
}

// var locations map[int]location

// type visitsType struct {
// 	Visits []visit `json:"visits"`
// }

type visit struct {
	ID        int `json:"id"`
	Location  int `json:"location"`
	User      int `json:"user"`
	VisitedAt int `json:"visited_at"`
	Mark      int `json:"mark"`
}

// var visits map[int]visit

const dsn = "file::memory:?cache=shared"

// const dsn = "file:test.db?mode=memory&cache=shared"
// const dsn = "file:test.db?cache=shared"

func init() {
	// users = make(map[int]user, 0)
	// locations = make(map[int]location, 0)
	// visits = make(map[int]visit, 0)

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		panic(err)
	}

	db.Exec(`
	create table users (
		id integer,
		email text,
		first_name text,
		last_name text,
		gender text,
		birth_date integer
	)
	`)

	db.Exec(`
	create table locations (
		id integer,
		place text,
		country text,
		city text,
		distance integer
	)
	`)

	db.Exec(`
	create table visits (
		id integer,
		location integer,
		user integer,
		visited_at integer,
		mark integer
	)
	`)
}

func loadData(archivePath string) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(rc)
		if err != nil {
			return err
		}

		log.Println("load", f.Name)

		switch {
		case strings.HasPrefix(f.Name, "users"):
			var data struct {
				Users []user `json:"users"`
			}
			if err = json.Unmarshal(b, &data); err != nil {
				return err
			}
			for _, user := range data.Users {
				tx.Exec("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)", user.ID, user.Email, user.FirstName, user.LastName, user.Gender, user.BirthDate)
			}
		case strings.HasPrefix(f.Name, "locations"):
			var data struct {
				Locations []location `json:"locations"`
			}
			if err = json.Unmarshal(b, &data); err != nil {
				return err
			}
			for _, location := range data.Locations {
				tx.Exec("INSERT INTO locations VALUES (?, ?, ?, ?, ?)", location.ID, location.Place, location.Country, location.City, location.Distance)
			}
		case strings.HasPrefix(f.Name, "visits"):
			var data struct {
				Visits []visit `json:"visits"`
			}
			if err = json.Unmarshal(b, &data); err != nil {
				return err
			}
			for _, visit := range data.Visits {
				tx.Exec("INSERT INTO visits VALUES (?, ?, ?, ?, ?)", visit.ID, visit.Location, visit.User, visit.VisitedAt, visit.Mark)
			}
		}
	}

	tx.Commit()

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

	log.Println("data load complete")

	router := httprouter.New()

	router.GET("/users/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")
		db, _ := sql.Open("sqlite3", dsn)
		var u user
		err := db.QueryRow("SELECT id, email, first_name, last_name, gender, birth_date FROM users WHERE id = ?", id).Scan(&u.ID, &u.Email, &u.FirstName, &u.LastName, &u.Gender, &u.BirthDate)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		b, _ := json.Marshal(u)
		w.Write(b)
	})

	router.GET("/locations/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")
		db, _ := sql.Open("sqlite3", dsn)
		var l location
		err := db.QueryRow("SELECT id, place, country, city, distance FROM locations WHERE id = ?", id).Scan(&l.ID, &l.Place, &l.Country, &l.City, &l.Distance)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		b, _ := json.Marshal(l)
		w.Write(b)
	})

	router.GET("/visits/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")
		db, _ := sql.Open("sqlite3", dsn)
		var v visit
		err := db.QueryRow("SELECT id, location, user, visited_at, mark FROM visits WHERE id = ?", id).Scan(&v.ID, &v.Location, &v.User, &v.VisitedAt, &v.Mark)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		b, _ := json.Marshal(v)
		w.Write(b)
	})

	router.GET("/users/:id/visits", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")

		q := r.URL.Query()

		db, err := sql.Open("sqlite3", dsn)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		sql := "select visits.mark, visits.visited_at, locations.place from visits join locations on visits.location = locations.id where visits.user = ? "

		args := make([]interface{}, 1)
		args[0] = id

		if len(q["fromDate"]) > 0 {
			fromDate, err := strconv.Atoi(q.Get("fromDate"))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			sql += "and visits.visited_at > ?"
			args = append(args, fromDate)
		}

		if len(q["toDate"]) > 0 {
			toDate, err := strconv.Atoi(q.Get("toDate"))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			sql += "and visits.visited_at < ?"
			args = append(args, toDate)
		}

		if len(q["country"]) > 0 {
			sql += "and locations.country = ?"
			args = append(args, q.Get("country"))
		}

		if len(q["toDistance"]) > 0 {
			toDistance, err := strconv.Atoi(q.Get("toDistance"))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			sql += "and distance < ?"
			args = append(args, toDistance)
		}

		rows, err := db.Query(sql, args...)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		type result struct {
			Mark      int    `json:"mark"`
			VisitedAt int    `json:"visited_at"`
			Place     string `json:"place"`
		}

		res := make([]result, 0)

		for rows.Next() {
			var mark int
			var visitedAt int
			var place string
			if err = rows.Scan(&mark, &visitedAt, &place); err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			res = append(res, result{mark, visitedAt, place})
		}

		b, _ := json.Marshal(res)
		w.Write(b)
	})

	// router.GET("/users/:id/visits", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// 	id, _ := strconv.Atoi(ps.ByName("id"))

	// 	if _, found := users[id]; !found {
	// 		http.NotFound(w, r)
	// 		return
	// 	}

	// 	q := r.URL.Query()

	// 	var fromDate *int
	// 	var toDate *int
	// 	var country *string
	// 	var toDistance *int

	// 	if len(q["fromDate"]) > 0 {
	// 		x, err := strconv.Atoi(q["fromDate"][0])
	// 		if err != nil {
	// 			w.WriteHeader(http.StatusBadRequest)
	// 		}
	// 		return

	// 		fromDate = &x
	// 	}

	// 	if len(q["toDate"]) > 0 {
	// 		x, err := strconv.Atoi(q["toDate"][0])
	// 		if err != nil {
	// 			w.WriteHeader(http.StatusBadRequest)
	// 		}
	// 		return

	// 		toDate = &x
	// 	}

	// 	if len(q["country"]) > 0 {
	// 		country = &q["country"][0]
	// 	}

	// 	if len(q["toDistance"]) > 0 {
	// 		x, err := strconv.Atoi(q["toDistance"][0])
	// 		if err != nil {
	// 			w.WriteHeader(http.StatusBadRequest)
	// 		}
	// 		return

	// 		toDistance = &x
	// 	}

	// 	resp := make([]struct {
	// 		Mark      int    `json:"mark"`
	// 		VisitedAt int    `json:"visited_at"`
	// 		Place     string `json:"place"`
	// 	}, 0)

	// 	b, _ := json.Marshal(resp)
	// 	w.Write(b)
	// })

	log.Fatal(http.ListenAndServe(":"+os.Getenv("PORT"), router))
}
