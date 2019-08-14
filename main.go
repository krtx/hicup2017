package main

import (
	"archive/zip"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
	_ "github.com/mattn/go-sqlite3"
)

type user struct {
	ID        int    `json:"id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Gender    string `json:"gender"`
	BirthDate int64  `json:"birth_date"`
}

type location struct {
	ID       int    `json:"id"`
	Place    string `json:"place"`
	Country  string `json:"country"`
	City     string `json:"city"`
	Distance int    `json:"distance"`
}

type visit struct {
	ID        int `json:"id"`
	Location  int `json:"location"`
	User      int `json:"user"`
	VisitedAt int `json:"visited_at"`
	Mark      int `json:"mark"`
}

const driver = "sqlite3"

const dsn = "file:test.db?mode=memory&cache=shared"

// const dsn = "file:test.db?cache=shared"

func init() {
	db, err := sql.Open(driver, dsn)
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

	db.Exec(`create index users_id on users (id)`)

	db.Exec(`
	create table locations (
		id integer,
		place text,
		country text,
		city text,
		distance integer
	)
	`)

	db.Exec(`create index locations_id on locations (id)`)

	db.Exec(`
	create table visits (
		id integer,
		location integer,
		user integer,
		visited_at integer,
		mark integer
	)
	`)

	db.Exec(`create index visits_location_user on visits (location, user)`)
}

func getIntValue(v url.Values, name string) (*int, error) {
	if len(v[name]) == 0 {
		return nil, nil
	}

	x, err := strconv.Atoi(v[name][0])
	if err != nil {
		return nil, err
	}

	return &x, nil
}

func loadData(archivePath string) error {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()

	db, err := sql.Open(driver, dsn)
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
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	err := loadData(os.Getenv("ARCHIVE_PATH"))
	if err != nil {
		panic(err)
	}

	log.Println("data load complete")

	router := httprouter.New()

	router.PanicHandler = func(w http.ResponseWriter, r *http.Request, err interface{}) {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	router.GET("/users/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		db, err := sql.Open(driver, dsn)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		id := ps.ByName("id")

		var u user
		err = db.QueryRow("SELECT id, email, first_name, last_name, gender, birth_date FROM users WHERE id = ?", id).Scan(&u.ID, &u.Email, &u.FirstName, &u.LastName, &u.Gender, &u.BirthDate)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		b, _ := json.Marshal(u)
		w.Write(b)
	})

	router.GET("/locations/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		db, err := sql.Open(driver, dsn)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		id := ps.ByName("id")

		var l location
		err = db.QueryRow("SELECT id, place, country, city, distance FROM locations WHERE id = ?", id).Scan(&l.ID, &l.Place, &l.Country, &l.City, &l.Distance)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		b, _ := json.Marshal(l)
		w.Write(b)
	})

	router.GET("/visits/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		db, err := sql.Open(driver, dsn)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		id := ps.ByName("id")

		var v visit
		err = db.QueryRow("SELECT id, location, user, visited_at, mark FROM visits WHERE id = ?", id).Scan(&v.ID, &v.Location, &v.User, &v.VisitedAt, &v.Mark)
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

		query := "select visits.mark, visits.visited_at, locations.place from visits join locations on visits.location = locations.id where visits.user = ? "

		args := make([]interface{}, 1)
		args[0] = id

		fromDate, err := getIntValue(q, "fromDate")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if fromDate != nil {
			query += "and visits.visited_at > ?"
			args = append(args, *fromDate)
		}

		toDate, err := getIntValue(q, "toDate")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if toDate != nil {
			query += "and visits.visited_at < ?"
			args = append(args, toDate)
		}

		if len(q["country"]) > 0 {
			query += "and locations.country = ?"
			args = append(args, q.Get("country"))
		}

		if len(q["toDistance"]) > 0 {
			toDistance, err := strconv.Atoi(q.Get("toDistance"))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			query += "and distance < ?"
			args = append(args, toDistance)
		}

		db, err := sql.Open(driver, dsn)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		rows, err := db.Query(query, args...)
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
				panic(err)
			}

			res = append(res, result{mark, visitedAt, place})
		}

		b, _ := json.Marshal(res)
		w.Write(b)
	})

	router.GET("/locations/:id/avg", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		id := ps.ByName("id")

		db, err := sql.Open(driver, dsn)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		var count int
		err = db.QueryRow("SELECT id FROM locations WHERE id = ?", id).Scan(&count)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		q := r.URL.Query()

		query := "SELECT SUM(visits.mark), COUNT(1) FROM visits JOIN users ON users.id = visits.user WHERE visits.location = ? "

		args := make([]interface{}, 1)
		args[0] = id

		fromDate, err := getIntValue(q, "fromDate")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if fromDate != nil {
			query += "AND visits.visited_at > ?"
			args = append(args, *fromDate)
		}

		toDate, err := getIntValue(q, "toDate")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if toDate != nil {
			query += "AND visits.visited_at < ?"
			args = append(args, toDate)
		}

		if len(q["gender"]) > 0 {
			query += "AND users.gender = ?"
			args = append(args, q.Get("gender"))
		}

		fromAge, err := getIntValue(q, "fromAge")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if fromAge != nil {
			query += "AND users.age > ?"
			args = append(args, fromAge)
		}

		toAge, err := getIntValue(q, "toAge")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if fromAge != nil {
			query += "AND users.age < ?"
			args = append(args, toAge)
		}

		var sum int
		err = db.QueryRow(query, args...).Scan(&sum, &count)
		if err != nil {
			panic(err)
		}

		fmt.Fprintf(w, "{\"avg\":%.5f}", float64(sum)/float64(count))
	})

	router.POST("/:entity/:id", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		entity := ps.ByName("entity")
		id := ps.ByName("id")

		if entity != "locations" && entity != "users" && entity != "visits" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		db, err := sql.Open(driver, dsn)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}

		if id == "new" {
			if entity == "users" {
				var u user
				if err = json.Unmarshal(b, &u); err != nil {
					panic(err)
				}
				db.QueryRow("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)", u.ID, u.Email, u.FirstName, u.LastName, u.Gender, u.BirthDate)
			} else if entity == "locations" {
				var u location
				if err = json.Unmarshal(b, &u); err != nil {
					panic(err)
				}
				db.QueryRow("INSERT INTO locations VALUES (?, ?, ?, ?, ?)", u.ID, u.Place, u.Country, u.City, u.Distance)
			} else if entity == "visits" {
				var u visit
				if err = json.Unmarshal(b, &u); err != nil {
					panic(err)
				}
				db.QueryRow("INSERT INTO visits VALUES (?, ?, ?, ?, ?)", u.ID, u.Location, u.User, u.VisitedAt, u.Mark)
			}
			fmt.Fprintf(w, "{}")
			return
		}

		var v int
		err = db.QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE id = ?", entity), id).Scan(&v)
		if err != nil {
			http.NotFound(w, r)
			return
		}

		if entity == "users" {
			var u user
			if err = json.Unmarshal(b, &u); err != nil {
				panic(err)
			}
			db.QueryRow("UPDATE users SET email = ?, first_name = ?, last_name = ?, birth_date = ? WHERE id = ?", u.Email, u.FirstName, u.LastName, u.BirthDate, id)
		} else if entity == "locations" {
			var u location
			if err = json.Unmarshal(b, &u); err != nil {
				panic(err)
			}
			db.QueryRow("UPDATE locations SET place = ?, country = ?, city = ?, distance = ? WHERE id = ?", u.Place, u.Country, u.City, u.Distance, id)
		} else if entity == "visits" {
			var u visit
			if err = json.Unmarshal(b, &u); err != nil {
				panic(err)
			}
			db.QueryRow("UPDATE visits SET loation = ?, user = ?, visited_at = ?, mark = ? WHERE id = ?", u.Location, u.User, u.VisitedAt, u.Mark, id)
		}
		fmt.Fprintf(w, "{}")
	})

	log.Fatal(http.ListenAndServe(":"+os.Getenv("PORT"), router))
}
