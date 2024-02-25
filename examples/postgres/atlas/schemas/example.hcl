table "event_outbox" {
  schema = schema.strongforce
  column "id" {
    null = false
    type = char(36)
  }
  column "topic" {
    null = false
    type = varchar(255)
  }
  column "payload" {
    null = false
    type = bytea
  }
  column "created_at" {
    null    = true
    type    = timestamp
    default = sql("CURRENT_TIMESTAMP")
  }
  primary_key {
    columns = [column.id]
  }
}

table "test" {
  schema = schema.strongforce
  column "id" {
    type = serial
  }
  column "value" {
    null = false
    type = varchar(255)
  }
  primary_key {
    columns = [column.id]
  }
}
