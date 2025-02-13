CREATE DATABASE movies;
USE movies;

CREATE TABLE imdb (
    imdb_id varchar(16) not null,
    vote_average float default '0',
    vote_count int default '0',
    release_date date,
    revenue decimal(15,2) default '1000000',
    budget decimal(15,2) default '1000000',
    runtime int default '90',

    constraint pk_imdb_id primary key (imdb_id)
);