-- Databricks notebook source
CREATE OR REPLACE TABLE mvp_engenharia_de_dados.bronze.market_value AS
SELECT *, current_timestamp() AS _ingested_at, 'market_value.csv' AS _source_file
FROM read_files('/Volumes/mvp_engenharia_de_dados/bronze/raw_files/market_value.csv',
  format => 'csv', header => true, inferSchema => true);

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.bronze.performance_tm AS
SELECT *, current_timestamp() AS _ingested_at, 'performance_tm.csv' AS _source_file
FROM read_files('/Volumes/mvp_engenharia_de_dados/bronze/raw_files/performance_tm.csv',
  format => 'csv', header => true, inferSchema => true);

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.bronze.players_tm AS
SELECT
  Clube,
  Nome,
  `URL do perfil` AS url_do_perfil,
  ID,
  current_timestamp() AS _ingested_at,
  'players_tm.csv' AS _source_file
FROM read_files('/Volumes/mvp_engenharia_de_dados/bronze/raw_files/players_tm.csv',
  format => 'csv', header => true, inferSchema => true);

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.bronze.partidas_sofascore AS
SELECT
  Data,
  Campeonato,
  `ID da Partida` AS id_da_partida,
  current_timestamp() AS _ingested_at,
  'partidas_sofascore.csv' AS _source_file
FROM read_files('/Volumes/mvp_engenharia_de_dados/bronze/raw_files/partidas_sofascore.csv',
  format => 'csv', header => true, inferSchema => true);

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.bronze.statistics_game AS
SELECT *, current_timestamp() AS _ingested_at, 'statistics_game.csv' AS _source_file
FROM read_files('/Volumes/mvp_engenharia_de_dados/bronze/raw_files/statistics_game.csv',
  format => 'csv', header => true, inferSchema => true);

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.bronze.statistics_player AS
SELECT *, current_timestamp() AS _ingested_at, 'statistics_player.csv' AS _source_file
FROM read_files('/Volumes/mvp_engenharia_de_dados/bronze/raw_files/statistics_player.csv',
  format => 'csv', header => true, inferSchema => true);