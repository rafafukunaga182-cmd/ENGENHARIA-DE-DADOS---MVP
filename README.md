# MVP — Engenharia de Dados: Valor de Mercado x Desempenho no Futebol Brasileiro (2024) - PUC-RIO

**Autor:** Rafael Akira Fukunaga
**Plataforma:** Databricks 

## Sumário
-  [Contexto de Negócios e Perguntas](#contexto-de-negócios-e-perguntas-etapa-2-e-41)
-  [Carga dos Dados](#carga-dos-dados-etapa-42)
-  [Modelagem e Catálogo de Dados](#modelagem-e-catálogo-de-dados-etapa-43)
-  [Pipeline de Dados ](#pipeline-de-dados-etapa-44)
-  [Qualidade de Dados](#qualidade-de-dados-etapa-45)
-  [Análise de Dados](#análise-de-dados-etapa-45)
-  [Autoavaliação](#autoavaliação)

---

## Contexto de Negócios e Perguntas

### Problema de negócio

Investigar se o valor de mercado dos jogadores do futebol brasileiro reflete o desempenho real em campo, cruzando dados de mercado e carreira (Transfermarkt) com estatísticas de partidas da temporada 2024 (Sofascore).

### Perguntas de negócio

1. Existe correlação entre o valor de mercado atual do jogador e sua média de *rating* (Sofascore) na temporada 2024?
2. Jogadores com maior valor de mercado têm maior participação em gols (gols + assistências) por 90 minutos jogados?
3. Clubes com elenco mais valioso (soma do valor de mercado dos jogadores) têm melhor desempenho coletivo (posse de bola, xG, pontos por jogo) no Brasileirão 2024?
4. A idade influencia o valor de mercado e o desempenho ao mesmo tempo — existe uma janela de idade onde o custo-benefício é maior?
5. Jogadores mais valorizados jogam mais minutos, e isso se reflete em desempenho médio melhor?

### Contexto e estrutura dos dados brutos

Os dados foram obtidos por meio de um dataset público do Kaggle "Sofascore and Transfermarkt Football 2024" (https://www.kaggle.com/datasets/felipesembay/sofascore-and-transfermarkt-football-data), que compila informações originalmente extraídas (via scraping) do Transfermarkt e do Sofascore. O dataset chegou como 6 arquivos CSV, descritos abaixo.

**Fonte 1 — Transfermarkt** (histórico de carreira e valor de mercado, 2004–2024)

| Arquivo | Linhas | Grão | Principais colunas |
|---|---|---|---|
| `market_value.csv` | 33.043 | 1 avaliação de valor por jogador por data | `player_id`, `player_name`, `datum_mw`, `y` (valor em €), `age`, `verein` (clube na data) |
| `performance_tm.csv` | 9.596 | 1 registro por jogador × temporada × competição | `player_id`, `player_name`, `nameSeason`, `competitionDescription`, `gamesPlayed`, `goalsScored`, `assists`, `minutesPlayed`, cartões |
| `players_tm.csv` | 2.729 | 1 jogador do elenco atual por clube | `Clube`, `ID`, `URL do perfil` (coluna `Nome` veio 100% vazia e foi descartada) |

Cobre 2.569 a 2.650 jogadores distintos, valor de mercado de 04/10/2004 a 16/10/2024, e 99 competições diferentes (não só o Brasileirão — inclui ligas e copas onde jogadores brasileiros atuam).

**Fonte 2 — Sofascore** (estatísticas de partidas da temporada 2024)

| Arquivo | Linhas | Grão | Principais colunas |
|---|---|---|---|
| `partidas_sofascore.csv` | 634 | 1 partida | `Data`, `Campeonato`, `ID da Partida` |
| `statistics_game.csv` | 102.690 | 1 métrica × time × partida (formato longo) | `id_partida`, `data_jogo`, `campeonato`, `home_team`, `away_team`, `period`, `group`, `key`, `home_value`, `away_value`, `resultado` |
| `statistics_player.csv` | 39.861 | 1 jogador × partida | `id_partida`, `team_name`, `name`, `position`, `rating`, `goals`, `expectedGoals`, `minutesPlayed`, entre outras ~40 métricas |

Cobre 880 partidas (02/04/2024 a 12/10/2024), 21 campeonatos (Brasileirão Série A e B, Copa do Brasil, Libertadores, Sudamericana) e 3.027 jogadores distintos. `partidas_sofascore.csv` acabou não sendo usado na modelagem final: `statistics_game.csv` já contém `id_partida`, `data_jogo`, `campeonato`, `home_team` e `away_team`, tornando o arquivo redundante (foi mantido na Bronze por rastreabilidade).

### Licença de uso

O dataset foi obtido da plataforma Kaggle, sob a licença Database Contents License (DbCL) v1.0, indicada na página do dataset (https://www.kaggle.com/datasets/felipesembay/sofascore-and-transfermarkt-football-data)


## 2- Carga dos Dados

### Como foi feita

1. Os 6 arquivos CSV foram obtidos pelo Kaggle (https://www.kaggle.com/datasets/felipesembay/sofascore-and-transfermarkt-football-data)
2. Conta criada no **Databricks Free Edition**.
3. Estrutura de catálogo criada seguindo a Arquitetura Medalhão:

```sql
CREATE CATALOG IF NOT EXISTS mvp_engenharia_de_dados;
CREATE SCHEMA IF NOT EXISTS mvp_engenharia_de_dados.bronze;
CREATE SCHEMA IF NOT EXISTS mvp_engenharia_de_dados.silver;
CREATE SCHEMA IF NOT EXISTS mvp_engenharia_de_dados.gold;
CREATE VOLUME IF NOT EXISTS mvp_engenharia_de_dados.bronze.raw_files;
```

4. Os 6 CSVs foram enviados para o Volume `mvp_engenharia_de_dados.bronze.raw_files` via upload no Catalog Explorer.
5. Cada arquivo foi lido com a função `read_files()` e gravado como tabela Delta na camada **Bronze**, preservando os dados exatamente como vieram da fonte, com metadados de rastreabilidade (`_ingested_at`, `_source_file`). Duas colunas com espaço no nome (`URL do perfil`, `ID da Partida`) precisaram ser renomeadas no próprio `SELECT`, pois o Delta Lake não aceita espaços em nomes de coluna sem habilitar Column Mapping.

### Script de referência

Notebook `01_bronze_ingestion` — (https://github.com/rafafukunaga182-cmd/ENGENHARIA-DE-DADOS---MVP/blob/main/01_bronze_ingestion.sql)

---

## 3- Modelagem e Catálogo de Dado

### Abordagem de modelagem

Foi adotado um esquema de duas dimensões centrais (`dim_jogador`, `dim_time`) compartilhadas por quatro tabelas fato, cada uma respondendo a um subconjunto das 5 perguntas de negócio. Essa abordagem foi escolhida em vez de um Esquema Estrela único porque as perguntas operam em granularidades diferentes (avaliação de valor, temporada, partida-time, partida-jogador), que não cabem numa única tabela fato sem gerar redundância.

O maior desafio da modelagem foi que as duas fontes **não compartilham uma chave comum**: o Transfermarkt identifica jogadores e clubes por ID/nome oficial, enquanto o Sofascore só traz nomes e apelidos como aparecem em campo. Esse problema foi resolvido na camada Silver através de duas tabelas de cruzamento (*de-para*), detalhadas na seção de Qualidade de Dados.

### Estrutura das tabelas (camada Gold)

**Dimensões**

| Tabela | Grão | Descrição |
|---|---|---|
| `dim_jogador` | 1 linha por jogador do Sofascore | Chave estável do jogador, com vínculo ao Transfermarkt quando identificado |
| `dim_time` | 1 linha por time do Sofascore | Nome curto (Sofascore) e nome oficial (Transfermarkt) |
| `dim_data` | 1 linha por dia (2000–2025) | Dimensão calendário padrão |
| `dim_partida` (`silver.partida`, usada como dimensão na Gold) | 1 linha por partida | Campeonato, times, data, resultado |

**Fatos**

| Tabela | Grão | Perguntas que responde |
|---|---|---|
| `fato_valor_mercado` | jogador × data de avaliação | 1, 3, 4 |
| `fato_desempenho_temporada` | jogador × temporada × competição | 5 (complementar) |
| `fato_estatisticas_time_partida` | time × partida | 3 |
| `fato_estatisticas_jogador_partida` | jogador × partida | 1, 2, 5 |

### Catálogo de Dados

#### `dim_jogador`

| Campo | Tipo | Descrição | Domínio | Linhagem |
|---|---|---|---|---|
| `id_jogador` | string (PK) | Chave estável do jogador | ID numérico do Transfermarkt quando identificado, ou código gerado `SOFA_<hash>` quando não | Gerado na Gold a partir do cruzamento de nomes |
| `id_jogador_tm` | string (nullable) | ID original do jogador no Transfermarkt | Numérico ou nulo | Bronze `market_value.player_id` |
| `nome` | string | Nome do jogador como aparece no Sofascore | Texto livre | Bronze `statistics_player.name` |
| `posicao_principal` | string | Posição mais frequente nas partidas observadas | G, D, M, F | Calculado (moda) a partir de `statistics_player.position` |
| `status_match` | string | Confiabilidade do vínculo com o Transfermarkt | `match_seguro`, `match_resolvido_por_clube`, `nao_identificado` | Gerado no processo de resolução de nomes (Silver) |

#### `dim_time`

| Campo | Tipo | Descrição | Domínio | Linhagem |
|---|---|---|---|---|
| `id_time` | string (PK) | Nome do time como aparece no Sofascore | ~95 valores distintos | Bronze `statistics_game.home_team`/`away_team` |
| `nome_curto` | string | Idêntico a `id_time` | — | — |
| `nome_oficial` | string (nullable) | Nome oficial completo do clube no Transfermarkt | Texto livre; nulo para 5 clubes não localizados na fonte | Bronze `players_tm.Clube`, casado por sobreposição de tokens |
| `status_match` | string | Confiabilidade do vínculo | `match_automatico`, `nao_encontrado_na_fonte` | Gerado no cruzamento de clubes (Silver) |

#### `dim_data`

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| `dt` | date (PK) | Data calendário | 2000-01-01 a 2025-12-31 |
| `ano`, `mes`, `trimestre` | int | Derivados de `dt` | — |
| `dia_semana` | string | Nome do dia da semana | Monday…Sunday |

#### `dim_partida` (tabela `silver.partida`)

| Campo | Tipo | Descrição | Domínio | Linhagem |
|---|---|---|---|---|
| `id_partida` | bigint (PK) | Identificador único da partida no Sofascore | — | Bronze `statistics_game.id_partida` |
| `dt_jogo` | date | Data do jogo | 02/04/2024 a 12/10/2024 | Bronze `statistics_game.data_jogo` |
| `campeonato` | string | Competição | 21 valores (Brasileirão A/B, Copa do Brasil, Libertadores, Sudamericana) | Bronze |
| `home_team`, `away_team` | string | Times mandante/visitante | FK `dim_time.id_time` | Bronze |
| `resultado` | string (nullable) | Resultado da partida | Ex.: `"Corinthians venceu"`, `"Empate"` | Bronze `statistics_game.resultado`, deduplicado (o campo vinha repetido em toda linha de estatística de uma mesma partida) |

#### `fato_valor_mercado`

| Campo | Tipo | Descrição | Domínio | Linhagem |
|---|---|---|---|---|
| `id_jogador_tm` | string (FK) | Jogador no Transfermarkt | — | Bronze `market_value.player_id` |
| `dt_avaliacao` | date | Data da avaliação de valor | 04/10/2004 a 16/10/2024 | Bronze `market_value.datum_mw` |
| `valor_eur` | decimal(15,2) | Valor de mercado em euros | €0 a €150.000.000 (média ≈ €1.956.403) | Bronze `market_value.y`, sem transformação de escala |
| `idade_na_data` | int | Idade do jogador na data da avaliação | 13 a 43 anos | Bronze `market_value.age` |
| `clube_na_data` | string | Clube do jogador na data (nome longo, Transfermarkt) | Texto livre | Bronze `market_value.verein` |

#### `fato_desempenho_temporada`

| Campo | Tipo | Descrição | Domínio | Linhagem |
|---|---|---|---|---|
| `id_jogador_tm` | string (FK) | Jogador no Transfermarkt | — | Bronze `performance_tm.player_id` |
| `temporada` | string | Temporada | 2 valores observados | Bronze `performance_tm.nameSeason` |
| `competicao` | string | Competição | 99 valores | Bronze `performance_tm.competitionDescription` |
| `jogos` | int | Jogos disputados | 0 a 32 | Bronze `performance_tm.gamesPlayed` |
| `gols`, `assistencias` | int | Gols e assistências na temporada | gols: 0 a 14 | Bronze `performance_tm.goalsScored`/`assists` |
| `cartoes_amarelos`, `cartoes_vermelhos` | int | Cartões na temporada | — | Bronze |
| `minutos_jogados` | int | Minutos jogados na temporada | 0 a 2.880 | Bronze `performance_tm.minutesPlayed` |

#### `fato_estatisticas_time_partida`

| Campo | Tipo | Descrição | Domínio | Linhagem |
|---|---|---|---|---|
| `id_partida` | bigint (FK) | Partida | — | `silver.partida` |
| `time` | string (FK) | Time (mandante ou visitante) | FK `dim_time.id_time` | Bronze `statistics_game.home_team`/`away_team` |
| `posse_bola` | double | Posse de bola do time na partida | 0 a 100 (%) | Bronze, `key = 'ballPossession'`, pivotado de linha pra coluna |
| `xg` | double | Expected Goals do time na partida | ≥ 0 | Bronze, `key = 'expectedGoals'` |
| `chutes_total`, `chutes_no_gol`, `escanteios`, `faltas`, `cartoes_amarelos`, `cartoes_vermelhos`, `grandes_chances_criadas`, `desarmes`, `interceptacoes` | double/int | Demais métricas de partida | ≥ 0 | Bronze, cada uma vinda de um `key` específico, pivotado |
| `resultado`, `campeonato`, `dt_jogo` | string/date | Herdados da partida | — | `silver.partida` |

#### `fato_estatisticas_jogador_partida`

| Campo | Tipo | Descrição | Domínio | Linhagem |
|---|---|---|---|---|
| `id_partida` | bigint (FK) | Partida | — | `silver.partida` |
| `id_jogador` | string (FK) | Jogador | FK `dim_jogador.id_jogador` | Gerado no cruzamento |
| `id_jogador_tm` | string (FK, nullable) | Jogador no Transfermarkt, quando identificado | — | `silver.de_para_jogador_resolvido` |
| `id_time` | string (FK) | Time do jogador na partida | FK `dim_time.id_time` | Bronze `statistics_player.team_name` |
| `position` | string | Posição na partida | G, D, M, F | Bronze `statistics_player.position` |
| `minutos_jogados` | int | Minutos jogados na partida | 1 a 90 | Bronze `statistics_player.minutesPlayed` |
| `rating` | double (nullable, 31,3% nulo) | Nota de desempenho do Sofascore | 3,0 a 10,0 (média ≈ 6,88) | Bronze `statistics_player.rating` |
| `gols`, `assistencias` | int | Gols e assistências na partida | — | Bronze `statistics_player.goals`/`goalAssist` |
| `xg`, `xa` | double | Expected Goals / Expected Assists individuais | xg: 0,003 a 2,019 | Bronze `statistics_player.expectedGoals`/`expectedAssists` |
| `passes_totais`, `passes_certos` | int | Passes tentados e certos | — | Bronze `statistics_player.totalPass`/`accuratePass` |

### Screenshots

<img width="891" height="440" alt="image" src="https://github.com/user-attachments/assets/6798eb45-b4c3-4bad-8a90-1c4c6190d937" />
<img width="895" height="589" alt="image" src="https://github.com/user-attachments/assets/693228ba-52bb-416f-93ff-d11be8310e09" />
<img width="891" height="734" alt="image" src="https://github.com/user-attachments/assets/574e51c4-7cdc-46ff-99a6-23449c1f4907" />
<img width="747" height="643" alt="image" src="https://github.com/user-attachments/assets/1b147fe8-fe79-4c24-ab85-e39bc353d76a" />
---

## Pipeline de Dados 

### Organização

O pipeline foi ramificado em **3 notebooks sequenciais**, um por camada da Arquitetura Medalhão, todos rodando em compute serverless do Databricks Free Edition:

| Notebook | Camada | Responsabilidade |
|---|---|---|
| `01_bronze_ingestion` | Bronze | Leitura dos 6 CSVs do Volume e gravação como tabelas Delta, sem alterações de conteúdo |
| `02_silver_modelagem` | Silver | Limpeza, tipagem, padronização e construção dos dois cruzamentos (*de-para* de jogador e de time) |
| `03_gold_modelagem` | Gold | Resolução final dos vínculos ambíguos, construção das dimensões e das 4 tabelas fato |

### Transformações principais documentadas

- **Ingestão Bronze**: leitura via `read_files()` com `header=true, inferSchema=true`; renomeação pontual de 2 colunas com espaço no nome (incompatíveis com Delta Lake).
- **Extração da dimensão de partida**: `statistics_game` repetia `campeonato`, `home_team`, `away_team` e `resultado` em toda linha de estatística de uma mesma partida. Resolvido agrupando por `id_partida` e usando `first()`/`max()` para colapsar a redundância numa única linha por jogo.
- **Filtragem de período**: `statistics_game` guarda cada métrica 3 vezes (jogo inteiro `ALL`, `1ST` e `2ND` tempo). Mantido apenas `period = 'ALL'` na Silver, por ser o nível de granularidade relevante para as perguntas do objetivo.
- **Cruzamento de jogador (nome → ID)**: normalização de acentuação/caixa, seguida de match exato; nomes ambíguos (que correspondem a mais de um `player_id`) foram isolados e resolvidos posteriormente na Gold usando o clube do jogador como critério de desempate.
- **Cruzamento de time (Sofascore → Transfermarkt)**: comparação por sobreposição de palavras (tokens), ignorando termos genéricos como "clube", "esporte", "futebol"; resultado explícito em `status_match`.
- **Pivot de `estatisticas_partida_time`**: transformação de formato longo (uma linha por métrica) para formato largo (uma linha por time × partida, com cada métrica como coluna), usando `PIVOT` do Spark SQL, após separar mandante/visitante em linhas independentes.
- Todas as tabelas usam `CREATE OR REPLACE TABLE`, tornando o pipeline idempotente: reexecutar o notebook do zero não duplica dados.

### Scripts e evidências

- Notebooks: [01_bronze_ingestion](https://github.com/rafafukunaga182-cmd/ENGENHARIA-DE-DADOS---MVP/blob/main/01_bronze_ingestion.sql), [02_silver_modelagem](https://github.com/rafafukunaga182-cmd/ENGENHARIA-DE-DADOS---MVP/blob/main/02_silver_modelagem.sql), [03_gold_modelagem](https://github.com/rafafukunaga182-cmd/ENGENHARIA-DE-DADOS---MVP/blob/main/03_gold_modelagem.sql)
- Screenshots:
 <img width="371" height="571" alt="image" src="https://github.com/user-attachments/assets/346df4c2-6957-42be-b9ea-99a2240859b1" />


---

## Qualidade de Dados (Etapa 4.5)

### Completude

- `rating` em `statistics_player`: **31,3% nulo** — ocorre principalmente para jogadores com pouquíssimos minutos em campo (entradas de última hora), que o Sofascore não pontua. Tratado mantendo o nulo (não há valor "certo" para inferir) e aplicando filtro de minutos mínimos nas análises que dependem de rating.
- `height` em `statistics_player`: 2,9% nulo — sem impacto nas perguntas do objetivo, não tratado.
- `nome_oficial` em `dim_time`: nulo para 5 dos 95 times (Fortaleza, CRB, Belgrano, Caracas F.C., Águia de Marabá) — genuinamente ausentes da fonte Transfermarkt raspada, não um erro de cruzamento. Documentado como lacuna conhecida, não preenchido artificialmente.

### Consistência

- Nomes de jogador e de clube vinham em formatos diferentes entre as duas fontes (acentuação, abreviações, ordem de palavras — ex.: "Sport Recife" vs. "Sport Club do Recife"). Resolvido via normalização de acentos/caixa e comparação por tokens (ver Modelagem).
- Datas em `market_value.datum_mw` vinham como texto em inglês (`"Apr 1, 2008"`), convertidas para tipo `date` na Silver.

### Unicidade

- **137 nomes normalizados (12,2% do universo de jogadores do Transfermarkt)** correspondem a mais de um `player_id` — nomes comuns no futebol brasileiro como "Rafael", "Marcelo", "Fabio", "Rafinha". Um cruzamento ingênuo por nome geraria vínculos errados nesses casos. Resolvido isolando esses nomes (`status_match = 'ambiguo_precisa_clube'`) e desambiguando pelo clube do jogador na partida.
- Não foram encontradas linhas duplicadas nas chaves naturais das tabelas de origem (`id_partida`, `player_id`).

### Acurácia

- Faixas de valores plausíveis em todos os campos numéricos centrais: idade entre 13 e 43 anos, valor de mercado entre €0 e €150.000.000 (compatível com os valores máximos conhecidos do mercado global na época da coleta), rating entre 3,0 e 10,0 — sem outliers evidentes de erro de digitação.

### Resultado do processo de cruzamento (resumo quantitativo)

| Cruzamento | Resultado |
|---|---|
| Jogador — casamento exato de nome | 1.836 / 3.027 (60,7%) |
| Jogador — casamento após normalizar acentos | 2.026 / 3.027 (66,9%) |
| Jogador — nomes ambíguos (múltiplos candidatos) | 137 nomes normalizados (12,2% do universo Transfermarkt) |
| Jogador — teste de correspondência aproximada (*fuzzy matching*) sem uso do clube | Descartado: recuperava apenas +5,7%, com risco real de falso positivo (ex.: juntou "Ruan Santos" com "Luan Santos", jogadores diferentes) |
| Jogador — resolução final (nome + clube) | Match seguro: 1879; Não identificados: 1148 |
| Time — casamento automático por sobreposição de palavras | 90 / 95 (94,7%) |
| Time — sem correspondência na fonte | 5 / 95 (5,3%) — Fortaleza, CRB, Belgrano, Caracas F.C., Águia de Marabá |

---

## Análise de Dados 

### Pergunta 1 — Existe correlação entre o valor de mercado atual do jogador e sua média de rating (Sofascore) na temporada 2024?

<img width="673" height="508" alt="image" src="https://github.com/user-attachments/assets/0e240b1f-6044-46dd-9c88-a84285bff7a1" />

**Resultado:** foi encontrada uma correlação de 0,28, considerada fraca, utilizando uma amostra de 1.158 jogadores, com no mínimo 5 partidas com rating registrado.

**Discussão:** Esse resultado mostra que existe uma relação positiva entre o valor de mercado e o rating médio, porém essa relação é baixa. O valor de mercado explica aproximadamente 8% da variação do rating (R² ≈ 0,08).
Isso indica que o valor de mercado de um jogador não depende apenas do seu desempenho em campo durante uma temporada. Outros fatores, como idade, potencial de desenvolvimento, histórico, reputação e exposição em outras competições também podem influenciar o valor do jogador.

### Pergunta 2 — Valor de mercado × participação em gols por 90 min

<img width="717" height="529" alt="image" src="https://github.com/user-attachments/assets/c175dcf3-a994-46d2-ae44-29fa92018fdd" />

**Resultado:** a correlação encontrada foi de 0,13, considerada muito fraca, utilizando uma amostra de 817 jogadores com pelo menos 450 minutos jogados.

**Discussão:** A correlação foi ainda menor do que na primeira pergunta. Isso mostra que o valor de mercado não está diretamente relacionado à participação em gols. Um dos motivos é que a participação em gols é uma métrica que favorece principalmente atacantes e jogadores mais ofensivos. Jogadores de outras posições, como zagueiros, volantes e goleiros, podem possuir um valor de mercado elevado mesmo sem participarem diretamente dos gols, devido a outras características de desempenho que não são consideradas nessa métrica.

### Pergunta 3 — Valor do elenco × desempenho coletivo do time

<img width="635" height="710" alt="image" src="https://github.com/user-attachments/assets/ccdc15d7-3eeb-48dc-b775-5249fb8fa40b" />

**Resultado (83 clubes):**

| Relação | Correlação |
|---|---|
| Valor do elenco × posse de bola média | 0,41 (moderada) |
| Valor do elenco × xG médio | 0,36 (moderada) |
| Valor do elenco × pontos por jogo | 0,47 (moderada) |

**Discussão:** Diferente das duas primeiras perguntas, os resultados apresentaram correlações moderadas. A maior correlação encontrada foi entre valor do elenco e pontos por jogo (0,47). Também foi encontrada uma relação de 0,41 com posse de bola e 0,36 com xG. Esse resultado mostra que, quando analisamos o valor de mercado no nível do elenco, a relação com o desempenho coletivo fica mais evidente. No nível individual, o valor de mercado apresentou correlações mais baixas, enquanto, quando os valores dos jogadores são agregados por clube, aparece uma relação maior com os indicadores de desempenho do time.

### Pergunta 4 — Idade × valor de mercado e desempenho

<img width="628" height="737" alt="image" src="https://github.com/user-attachments/assets/307b838e-a6e7-4f11-9084-6ebae0efe146" />

**Resultado:**

| Faixa etária | Jogadores | Valor médio (€) | Rating médio |
|---|---|---|---|
| 15–17 | 4 | 7.050.000 | 6,95 |
| 18–20 | 61 | 2.465.574 | 6,77 |
| 21–23 | 187 | 2.131.016 | 6,84 |
| 24–26 | 228 | 1.779.276 | 6,87 |
| 27–29 | 261 | 1.872.126 | 6,88 |
| 30–32 | 218 | 1.194.610 | 6,90 |
| 33–35 | 137 | 479.745 | 6,88 |
| 36–38 | 52 | 334.615 | 6,90 |
| 39–41 | 8 | 204.375 | 6,82 |
| 42+ | 2 | 225.000 | 6,91 |

**Discussão:** A faixa de 15 a 17 anos possui apenas 4 jogadores, então foi desconsiderada na interpretação por possuir uma amostra muito pequena. A partir dos 18 anos, é possível observar uma tendência de queda do valor de mercado conforme a idade aumenta. Por outro lado, o rating médio permanece relativamente estável, variando pouco entre as faixas analisadas. Um ponto interessante aparece entre 27 e 33 anos. Nessa faixa, os jogadores apresentam ratings médios próximos dos maiores valores encontrados, enquanto o valor de mercado já é consideravelmente menor do que nas faixas mais jovens. Isso indica uma possível relação entre idade, desempenho atual e valor de mercado. Jogadores mais jovens podem ter seu valor influenciado não apenas pelo desempenho atual, mas também pelo potencial de desenvolvimento e possibilidade de valorização futura.

### Pergunta 5 — Minutos jogados como elo entre valor e desempenho

<img width="613" height="471" alt="image" src="https://github.com/user-attachments/assets/4359e9d2-b75b-4ff9-a3b2-03080122d252" />

**Resultado:** a correlação entre valor de mercado e minutos jogados foi de 0,29, enquanto a correlação entre minutos jogados e rating médio foi de 0,41, utilizando uma amostra de 1.824 jogadores.

**Discussão:** Os resultados mostram que os minutos jogados possuem uma relação maior com o rating médio do que o próprio valor de mercado possui com a quantidade de minutos. Isso indica que a quantidade de minutos acumulados pode estar mais relacionada ao desempenho observado e à participação do jogador nas partidas do que diretamente ao seu valor de mercado.

### Discussão geral

De forma geral, os resultados mostram que o valor de mercado apresenta uma relação fraca com o desempenho individual dos jogadores. Nas análises individuais, as correlações ficaram entre 0,13 e 0,29. Por outro lado, quando o valor de mercado é analisado de forma agregada por clube, a relação com o desempenho coletivo aumenta, chegando a 0,47 na comparação com pontos por jogo. Outro ponto observado foi a influência da idade. Jogadores mais jovens apresentam valores de mercado maiores mesmo quando os ratings médios não apresentam uma diferença tão grande em relação aos jogadores mais velhos. Isso reforça a possibilidade de que o mercado considere fatores além do desempenho atual, como potencial de desenvolvimento e valorização futura. Com isso, os resultados indicam que o valor de mercado sozinho não é suficiente para explicar o desempenho de um jogador individualmente, mas pode apresentar uma relação mais clara quando utilizado para representar a qualidade geral de um elenco.


## Autoavaliação

Considero que os objetivos propostos para o MVP foram atingidos. As cinco perguntas de negócio definidas no início do projeto foram respondidas utilizando os dados do Transfermarkt e do Sofascore. Além da análise dos dados, também foi possível construir todo o fluxo de Engenharia de Dados no Databricks, desde a ingestão dos arquivos até a organização das tabelas nas camadas Bronze, Silver e Gold. Durante o desenvolvimento, também consegui aplicar conceitos de modelagem de dados, tratamento e qualidade dos dados, criação de pipelines e utilização de tabelas Delta. O projeto ajudou a entender melhor como essas etapas se conectam dentro de um projeto de Engenharia de Dados.

A principal dificuldade encontrada foi o cruzamento dos dados das duas fontes. O Transfermarkt possui um identificador próprio para os jogadores, enquanto o Sofascore não possui uma chave equivalente e utiliza os nomes dos jogadores. Além disso, os nomes poderiam aparecer com diferenças de acentuação, abreviações ou até apelidos. Para resolver esse problema, foi necessário criar um processo de normalização dos nomes e, nos casos em que existiam mais de um jogador com o mesmo nome, utilizar o clube como uma informação adicional para tentar identificar o jogador correto.

Outra dificuldade foi trabalhar com os dados de partidas do Sofascore, principalmente porque algumas informações apareciam repetidas em diferentes níveis de granularidade. Foi necessário entender a estrutura dos dados antes de realizar as transformações para evitar duplicidades e resultados incorretos.

Uma das principais limitações do projeto foi a quantidade de jogadores que não puderam ser relacionados entre as duas fontes. Após o processo de resolução, 1.879 jogadores foram identificados e 1.148 não puderam ser vinculados ao Transfermarkt. Isso significa que parte dos jogadores disponíveis no Sofascore não pôde ser utilizada nas análises que dependiam do valor de mercado. Além disso, cinco clubes presentes nos dados do Sofascore não foram encontrados na fonte do Transfermarkt. Outra limitação está relacionada ao período dos dados. As estatísticas do Sofascore utilizadas no projeto estão concentradas na temporada de 2024 e em um período específico de coleta. Dessa forma, os resultados representam esse recorte e não necessariamente podem ser generalizados para outras temporadas. Também foi necessário considerar a quantidade de valores nulos no rating do Sofascore. Como aproximadamente 31,3% dos registros possuem rating nulo, foi utilizado um filtro mínimo de partidas nas análises que dependiam dessa informação.

Como próximos passos, seria interessante melhorar o processo de identificação dos jogadores, principalmente utilizando um processo de fuzzy matching supervisionado, com revisão dos casos mais ambíguos. Isso poderia aumentar a quantidade de jogadores relacionados entre as duas fontes sem aumentar muito o risco de realizar correspondências incorretas. Também seria interessante expandir o projeto para outras temporadas, permitindo analisar se as relações encontradas em 2024 continuam aparecendo ao longo do tempo. Outra possibilidade seria adicionar mais informações sobre a posição e função tática dos jogadores. Dessa forma, seria possível comparar jogadores de posições semelhantes e evitar que uma mesma métrica seja utilizada da mesma forma para jogadores com funções muito diferentes em campo. Por fim, o projeto poderia evoluir para um pipeline atualizado periodicamente, permitindo acompanhar as mudanças no valor de mercado e no desempenho dos jogadores ao longo das temporadas.
