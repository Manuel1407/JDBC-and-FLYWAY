package com.amigoscode.movie;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class MovieDataAccessService implements MovieDao {

    private final JdbcTemplate jdbcTemplate;

    public MovieDataAccessService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<Movie> selectMovies() {
        var sql = """
                SELECT id, name, release_date 
                FROM movie
                LIMIT 100;
                """;
        List<Movie> movies = jdbcTemplate.query(sql, new MovieRowMapper()); // press control + space to show resultSet
        return movies;
    }

    @Override
    public int highestId(){
        var sql = """
                SELECT COALESCE(MAX(id), 0) + 1
                FROM movie;
                """;
        Integer highestId = jdbcTemplate.queryForObject(sql, Integer.class);

        return highestId != null ? highestId : 0;
    }

    // SELECT COALESCE(MAX(snapshot_id), 0) + 1 INTO snapshot_id FROM movie;

    @Override
    public int insertMovie(Movie movie) {
        var sql = """
                INSERT INTO movie(name, release_date)
                VALUES(?, ?);
                """;
        return jdbcTemplate.update(
                sql,
                movie.name(), movie.releaseDate()
        );
    }

    @Override
    public int deleteMovie(int id) {
        var sql = """
                DELETE FROM movie
                WHERE id = ?
                """;
        return jdbcTemplate.update(sql, id);
    }

    @Override
    public Optional<Movie> selectMovieById(int id) {
        var sql = """
                SELECT id, name, release_date 
                FROM movie
                WHERE id = ?
                """;
        return jdbcTemplate.query(sql, new MovieRowMapper(), id)
                .stream()
                .findFirst();
    }
    
}
