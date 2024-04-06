-- +goose Up
-- +goose StatementBegin
CREATE TABLE events (
                        id serial primary key,
                        level text,
                        microservice text,
                        ts float,
                        caller text,
                        msg text,
                        idLogger text,
                        fields text,
                        error text
)
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
drop table if exists events;
-- +goose StatementEnd
