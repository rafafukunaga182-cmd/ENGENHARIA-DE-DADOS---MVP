-- Databricks notebook source
-- MAGIC %md
-- MAGIC Passo 1: Padronizando as tabelas do Transfermarket

-- COMMAND ----------


CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.valor_mercado AS
SELECT
  player_id AS id_jogador_tm,
  player_name AS nome_jogador,
  to_date(datum_mw, 'MMM d, yyyy') AS dt_avaliacao,
  CAST(y AS DECIMAL(15,2)) AS valor_eur,
  CAST(age AS INT) AS idade_na_data,
  verein AS clube_na_data
FROM mvp_engenharia_de_dados.bronze.market_value
WHERE y IS NOT NULL;

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.desempenho_temporada AS
SELECT
  player_id AS id_jogador_tm,
  player_name AS nome_jogador,
  nameSeason AS temporada,
  competitionDescription AS competicao,
  CAST(gamesPlayed AS INT) AS jogos,
  CAST(goalsScored AS INT) AS gols,
  CAST(assists AS INT) AS assistencias,
  CAST(yellowCards AS INT) AS cartoes_amarelos,
  CAST(redCards AS INT) AS cartoes_vermelhos,
  CAST(minutesPlayed AS INT) AS minutos_jogados
FROM mvp_engenharia_de_dados.bronze.performance_tm;

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.elenco_atual AS
SELECT
  ID AS id_jogador_tm,
  Clube AS clube_atual,
  url_do_perfil
FROM mvp_engenharia_de_dados.bronze.players_tm;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Passo 2: Extraindo a dimensão de partida

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.partida AS
SELECT
  id_partida,
  to_date(first(data_jogo)) AS dt_jogo,
  first(campeonato) AS campeonato,
  first(home_team) AS home_team,
  first(away_team) AS away_team,
  max(resultado) AS resultado
FROM mvp_engenharia_de_dados.bronze.statistics_game
GROUP BY id_partida;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Passo 3: Estatísticas por time

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.estatisticas_partida_time AS
SELECT
  id_partida,
  home_team,
  away_team,
  key AS metrica,
  `group` AS categoria_metrica,
  CAST(home_value AS DOUBLE) AS valor_mandante,
  CAST(away_value AS DOUBLE) AS valor_visitante
FROM mvp_engenharia_de_dados.bronze.statistics_game
WHERE period = 'ALL';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Passo 4: Estatísticas por jogador

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.estatisticas_jogador_partida AS
SELECT
  id_partida,
  to_date(data_jogo) AS dt_jogo,
  campeonato,
  team_name,
  name AS nome_jogador_sofascore,
  position,
  CAST(height AS INT) AS altura_cm,
  CAST(minutesPlayed AS INT) AS minutos_jogados,
  CAST(rating AS DOUBLE) AS rating,
  CAST(goals AS INT) AS gols,
  CAST(goalAssist AS INT) AS assistencias,
  CAST(expectedGoals AS DOUBLE) AS xg,
  CAST(expectedAssists AS DOUBLE) AS xa,
  CAST(totalPass AS INT) AS passes_totais,
  CAST(accuratePass AS INT) AS passes_certos
FROM mvp_engenharia_de_dados.bronze.statistics_player;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Passo 5: Normalização de jogadores

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.jogadores_tm_normalizados AS
SELECT DISTINCT
  id_jogador_tm,
  nome_jogador,
  lower(translate(nome_jogador,
    'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
    'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS nome_normalizado
FROM (
  SELECT id_jogador_tm, nome_jogador FROM mvp_engenharia_de_dados.silver.valor_mercado
  UNION
  SELECT id_jogador_tm, nome_jogador FROM mvp_engenharia_de_dados.silver.desempenho_temporada
);

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.de_para_jogador AS
WITH nomes_ambiguos AS (
  SELECT nome_normalizado FROM mvp_engenharia_de_dados.silver.jogadores_tm_normalizados
  GROUP BY nome_normalizado HAVING count(DISTINCT id_jogador_tm) > 1
),
sofascore_norm AS (
  SELECT DISTINCT nome_jogador_sofascore,
    lower(translate(nome_jogador_sofascore,
      'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
      'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS nome_normalizado
  FROM mvp_engenharia_de_dados.silver.estatisticas_jogador_partida
  WHERE nome_jogador_sofascore IS NOT NULL
)
SELECT
  s.nome_jogador_sofascore,
  t.id_jogador_tm,
  t.nome_jogador AS nome_tm,
  CASE
    WHEN t.id_jogador_tm IS NULL THEN 'nao_identificado'
    WHEN s.nome_normalizado IN (SELECT nome_normalizado FROM nomes_ambiguos) THEN 'ambiguo_precisa_clube'
    ELSE 'match_seguro'
  END AS status_match
FROM sofascore_norm s
LEFT JOIN mvp_engenharia_de_dados.silver.jogadores_tm_normalizados t
  ON s.nome_normalizado = t.nome_normalizado
  AND s.nome_normalizado NOT IN (SELECT nome_normalizado FROM nomes_ambiguos);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Passo 6: Normalização times

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.de_para_time AS
WITH clubes_tm_tokens AS (
  SELECT DISTINCT
    clube_atual AS nome_tm,
    filter(
      split(lower(translate(clube_atual,
        'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')), '[\\s\\-\\(\\)]+'),
      t -> t NOT IN ('fc','ec','sc','ac','ca','cr','clube','esporte','esportiva',
                     'futebol','foot','ball','sociedade','associacao','regatas',
                     'de','do','da','dos','das','e','')
    ) AS tokens
  FROM mvp_engenharia_de_dados.silver.elenco_atual
),
clubes_sofascore AS (
  SELECT DISTINCT home_team AS nome_sofascore FROM mvp_engenharia_de_dados.silver.partida
  UNION
  SELECT DISTINCT away_team FROM mvp_engenharia_de_dados.silver.partida
),
candidatos AS (
  SELECT
    s.nome_sofascore,
    t.nome_tm,
    size(array_intersect(
      split(lower(translate(s.nome_sofascore,
        'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')), '[\\s\\-\\(\\)]+'),
      t.tokens
    )) AS tokens_em_comum
  FROM clubes_sofascore s
  CROSS JOIN clubes_tm_tokens t
),
melhores AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY nome_sofascore ORDER BY tokens_em_comum DESC) AS rn
  FROM candidatos
)
SELECT
  nome_sofascore,
  CASE WHEN tokens_em_comum = 0 THEN NULL ELSE nome_tm END AS nome_tm,
  tokens_em_comum,
  CASE WHEN tokens_em_comum = 0 THEN 'nao_encontrado_na_fonte' ELSE 'match_automatico' END AS status_match
FROM melhores
WHERE rn = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Passo 7: Validação

-- COMMAND ----------

SELECT status_match, count(*) AS qtd
FROM mvp_engenharia_de_dados.silver.de_para_jogador
GROUP BY status_match;

SELECT * FROM mvp_engenharia_de_dados.silver.de_para_time
ORDER BY tokens_em_comum ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

UPDATE mvp_engenharia_de_dados.silver.de_para_time
SET nome_tm = 'Caracas Fútbol Club'
WHERE nome_sofascore = 'Caracas F.C.';

UPDATE mvp_engenharia_de_dados.silver.de_para_time
SET nome_tm = 'Club Atlético Belgrano'
WHERE nome_sofascore = 'Belgrano';

UPDATE mvp_engenharia_de_dados.silver.de_para_time
SET nome_tm = 'Águia de Marabá Futebol Clube'
WHERE nome_sofascore = 'Águia de Marabá';

-- COMMAND ----------

update mvp_engenharia_de_dados.silver.jogadores_tm_normalizados
set status_match = 'match_seguro'
where status_match = 'nao_identificado';