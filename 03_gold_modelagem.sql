-- Databricks notebook source
-- MAGIC %md
-- MAGIC  1: Resolução de jogadores ambíguos

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.silver.de_para_jogador_resolvido AS
WITH ambiguos_com_time AS (
  SELECT DISTINCT d.nome_jogador_sofascore, e.team_name
  FROM mvp_engenharia_de_dados.silver.de_para_jogador d
  JOIN mvp_engenharia_de_dados.silver.estatisticas_jogador_partida e
    ON d.nome_jogador_sofascore = e.nome_jogador_sofascore
  WHERE d.status_match = 'ambiguo_precisa_clube'
),
candidatos AS (
  SELECT
    a.nome_jogador_sofascore,
    j.id_jogador_tm
  FROM ambiguos_com_time a
  JOIN mvp_engenharia_de_dados.silver.de_para_time dt ON a.team_name = dt.nome_sofascore
  JOIN mvp_engenharia_de_dados.silver.jogadores_tm_normalizados j
    ON lower(translate(a.nome_jogador_sofascore,
        'áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) = j.nome_normalizado
  JOIN mvp_engenharia_de_dados.silver.elenco_atual el ON j.id_jogador_tm = el.id_jogador_tm
  WHERE el.clube_atual = dt.nome_tm
),
resolvidos AS (
  SELECT nome_jogador_sofascore, id_jogador_tm,
    ROW_NUMBER() OVER (PARTITION BY nome_jogador_sofascore ORDER BY id_jogador_tm) AS rn,
    COUNT(*) OVER (PARTITION BY nome_jogador_sofascore) AS n_candidatos
  FROM candidatos
)
SELECT
  d.nome_jogador_sofascore,
  COALESCE(r.id_jogador_tm, d.id_jogador_tm) AS id_jogador_tm,
  CASE
    WHEN d.status_match = 'match_seguro' THEN 'match_seguro'
    WHEN r.id_jogador_tm IS NOT NULL AND r.n_candidatos = 1 THEN 'match_resolvido_por_clube'
    ELSE 'nao_identificado'
  END AS status_match
FROM mvp_engenharia_de_dados.silver.de_para_jogador d
LEFT JOIN resolvidos r ON d.nome_jogador_sofascore = r.nome_jogador_sofascore AND r.rn = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2: Dimensão do esquema de jogadores

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.gold.dim_jogador AS
WITH posicao_predominante AS (
  SELECT nome_jogador_sofascore, position,
    ROW_NUMBER() OVER (PARTITION BY nome_jogador_sofascore ORDER BY count(*) DESC) AS rn
  FROM mvp_engenharia_de_dados.silver.estatisticas_jogador_partida
  GROUP BY nome_jogador_sofascore, position
)
SELECT
  COALESCE(CAST(d.id_jogador_tm AS STRING), concat('SOFA_', md5(d.nome_jogador_sofascore))) AS id_jogador,
  d.id_jogador_tm,
  d.nome_jogador_sofascore AS nome,
  p.position AS posicao_principal,
  d.status_match
FROM mvp_engenharia_de_dados.silver.de_para_jogador_resolvido d
LEFT JOIN posicao_predominante p ON d.nome_jogador_sofascore = p.nome_jogador_sofascore AND p.rn = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Passo 4: Dimensão time e data

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.gold.dim_time AS
SELECT DISTINCT
  nome_sofascore AS id_time,
  nome_sofascore AS nome_curto,
  nome_tm AS nome_oficial,
  status_match
FROM mvp_engenharia_de_dados.silver.de_para_time;

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.gold.dim_data AS
SELECT
  dt,
  YEAR(dt) AS ano,
  MONTH(dt) AS mes,
  DATE_FORMAT(dt, 'EEEE') AS dia_semana,
  QUARTER(dt) AS trimestre
FROM (SELECT explode(sequence(to_date('2000-01-01'), to_date('2025-12-31'), interval 1 day)) AS dt);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5 - 

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.gold.fato_valor_mercado AS
SELECT CAST(id_jogador_tm AS STRING) AS id_jogador_tm, dt_avaliacao, valor_eur, idade_na_data, clube_na_data
FROM mvp_engenharia_de_dados.silver.valor_mercado;

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.gold.fato_desempenho_temporada AS
SELECT CAST(id_jogador_tm AS STRING) AS id_jogador_tm, temporada, competicao, jogos, gols,
  assistencias, cartoes_amarelos, cartoes_vermelhos, minutos_jogados
FROM mvp_engenharia_de_dados.silver.desempenho_temporada;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6-

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW estatisticas_longo_por_time AS
SELECT id_partida, home_team AS time, metrica, valor_mandante AS valor FROM mvp_engenharia_de_dados.silver.estatisticas_partida_time
UNION ALL
SELECT id_partida, away_team AS time, metrica, valor_visitante AS valor FROM mvp_engenharia_de_dados.silver.estatisticas_partida_time;

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.gold.fato_estatisticas_time_partida AS
SELECT e.*, p.resultado, p.campeonato, p.dt_jogo
FROM (
  SELECT * FROM estatisticas_longo_por_time
  PIVOT (
    first(valor) FOR metrica IN (
      'ballPossession' AS posse_bola, 'expectedGoals' AS xg,
      'totalShotsOnGoal' AS chutes_total, 'shotsOnGoal' AS chutes_no_gol,
      'cornerKicks' AS escanteios, 'fouls' AS faltas,
      'yellowCards' AS cartoes_amarelos, 'redCards' AS cartoes_vermelhos,
      'bigChanceCreated' AS grandes_chances_criadas,
      'totalTackle' AS desarmes, 'interceptionWon' AS interceptacoes
    )
  )
) e
JOIN mvp_engenharia_de_dados.silver.partida p ON e.id_partida = p.id_partida;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7-

-- COMMAND ----------

CREATE OR REPLACE TABLE mvp_engenharia_de_dados.gold.fato_estatisticas_jogador_partida AS
SELECT
  s.id_partida, dj.id_jogador, dj.id_jogador_tm, s.team_name AS id_time,
  s.position, s.minutos_jogados, s.rating, s.gols, s.assistencias,
  s.xg, s.xa, s.passes_totais, s.passes_certos
FROM mvp_engenharia_de_dados.silver.estatisticas_jogador_partida s
JOIN mvp_engenharia_de_dados.gold.dim_jogador dj ON s.nome_jogador_sofascore = dj.nome;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pergunta 1: Existe correlação entre o valor de mercado atual do jogador e sua média de rating (Sofascore) na temporada 2024?

-- COMMAND ----------

WITH valor_atual AS (
  SELECT id_jogador_tm, valor_eur,
    ROW_NUMBER() OVER (PARTITION BY id_jogador_tm ORDER BY dt_avaliacao DESC) AS rn
  FROM mvp_engenharia_de_dados.gold.fato_valor_mercado
),
rating_medio AS (
  SELECT id_jogador_tm, avg(rating) AS rating_medio, count(*) AS partidas
  FROM mvp_engenharia_de_dados.gold.fato_estatisticas_jogador_partida
  WHERE id_jogador_tm IS NOT NULL AND rating IS NOT NULL
  GROUP BY id_jogador_tm
  HAVING count(*) >= 5
)
SELECT corr(v.valor_eur, r.rating_medio) AS correlacao, count(*) AS jogadores_na_amostra
FROM valor_atual v
JOIN rating_medio r ON v.id_jogador_tm = r.id_jogador_tm
WHERE v.rn = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pergunta 2: 

-- COMMAND ----------


WITH valor_atual AS (
  SELECT id_jogador_tm, valor_eur,
    ROW_NUMBER() OVER (PARTITION BY id_jogador_tm ORDER BY dt_avaliacao DESC) AS rn
  FROM mvp_engenharia_de_dados.gold.fato_valor_mercado
),
participacao AS (
  SELECT id_jogador_tm,
    (SUM(gols) + SUM(assistencias)) / (SUM(minutos_jogados) / 90.0) AS participacao_por_90
  FROM mvp_engenharia_de_dados.gold.fato_estatisticas_jogador_partida
  WHERE id_jogador_tm IS NOT NULL
  GROUP BY id_jogador_tm
  HAVING SUM(minutos_jogados) >= 450
)
SELECT corr(v.valor_eur, p.participacao_por_90) AS correlacao, count(*) AS jogadores_na_amostra
FROM valor_atual v JOIN participacao p ON v.id_jogador_tm = p.id_jogador_tm
WHERE v.rn = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pergunta 3: 

-- COMMAND ----------

WITH valor_atual AS (
  SELECT id_jogador_tm, valor_eur,
    ROW_NUMBER() OVER (PARTITION BY id_jogador_tm ORDER BY dt_avaliacao DESC) AS rn
  FROM mvp_engenharia_de_dados.gold.fato_valor_mercado
),
valor_elenco AS (
  SELECT el.clube_atual, SUM(v.valor_eur) AS valor_total_elenco
  FROM mvp_engenharia_de_dados.silver.elenco_atual el
  JOIN valor_atual v ON el.id_jogador_tm = v.id_jogador_tm AND v.rn = 1
  GROUP BY el.clube_atual
),
desempenho_time AS (
  SELECT dt.nome_tm AS clube_atual,
    AVG(f.posse_bola) AS posse_media,
    AVG(f.xg) AS xg_medio,
    AVG(CASE
      WHEN f.resultado = CONCAT(f.time, ' venceu') THEN 3.0
      WHEN f.resultado = 'Empate' THEN 1.0
      ELSE 0.0
    END) AS pontos_por_jogo
  FROM mvp_engenharia_de_dados.gold.fato_estatisticas_time_partida f
  JOIN mvp_engenharia_de_dados.silver.de_para_time dt ON f.time = dt.nome_sofascore
  GROUP BY dt.nome_tm
)
SELECT
  corr(ve.valor_total_elenco, dtm.posse_media) AS corr_valor_posse,
  corr(ve.valor_total_elenco, dtm.xg_medio) AS corr_valor_xg,
  corr(ve.valor_total_elenco, dtm.pontos_por_jogo) AS corr_valor_pontos,
  count(*) AS clubes_na_amostra
FROM valor_elenco ve JOIN desempenho_time dtm ON ve.clube_atual = dtm.clube_atual;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pergunta 4

-- COMMAND ----------

WITH valor_atual AS (
  SELECT id_jogador_tm, valor_eur, idade_na_data,
    ROW_NUMBER() OVER (PARTITION BY id_jogador_tm ORDER BY dt_avaliacao DESC) AS rn
  FROM mvp_engenharia_de_dados.gold.fato_valor_mercado
),
rating_medio AS (
  SELECT id_jogador_tm, avg(rating) AS rating_medio
  FROM mvp_engenharia_de_dados.gold.fato_estatisticas_jogador_partida
  WHERE id_jogador_tm IS NOT NULL AND rating IS NOT NULL
  GROUP BY id_jogador_tm HAVING count(*) >= 5
)
SELECT
  FLOOR(v.idade_na_data / 3) * 3 AS faixa_etaria_inicio,
  COUNT(*) AS jogadores,
  ROUND(AVG(v.valor_eur), 0) AS valor_medio_eur,
  ROUND(AVG(r.rating_medio), 2) AS rating_medio_grupo
FROM valor_atual v JOIN rating_medio r ON v.id_jogador_tm = r.id_jogador_tm
WHERE v.rn = 1
GROUP BY FLOOR(v.idade_na_data / 3) * 3
ORDER BY faixa_etaria_inicio;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pergunta 5

-- COMMAND ----------

WITH valor_atual AS (
  SELECT id_jogador_tm, valor_eur,
    ROW_NUMBER() OVER (PARTITION BY id_jogador_tm ORDER BY dt_avaliacao DESC) AS rn
  FROM mvp_engenharia_de_dados.gold.fato_valor_mercado
),
uso AS (
  SELECT id_jogador_tm, SUM(minutos_jogados) AS minutos_total, AVG(rating) AS rating_medio
  FROM mvp_engenharia_de_dados.gold.fato_estatisticas_jogador_partida
  WHERE id_jogador_tm IS NOT NULL
  GROUP BY id_jogador_tm
)
SELECT
  corr(v.valor_eur, u.minutos_total) AS corr_valor_minutos,
  corr(u.minutos_total, u.rating_medio) AS corr_minutos_rating,
  count(*) AS jogadores_na_amostra
FROM valor_atual v JOIN uso u ON v.id_jogador_tm = u.id_jogador_tm
WHERE v.rn = 1;