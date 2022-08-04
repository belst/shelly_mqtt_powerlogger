create table power (
    timestamp timestamptz not null default current_timestamp,
    value double precision not null,
    primary key (timestamp)
);
