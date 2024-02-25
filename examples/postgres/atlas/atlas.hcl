lint {
  destructive {
    error = true
  }
  data_depend {
    error = true
  }
  incompatible {
    error = true
  }
}

env "local" {
  src = "file://schemas"
  url = "postgresql://strongforce:strongforce@127.0.0.1:64003/strongforce?sslmode=disable"
  dev = "docker://postgres/12/strongforce"
}
