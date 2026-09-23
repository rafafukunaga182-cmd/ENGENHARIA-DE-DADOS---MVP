# MVP — Engenharia de Dados: Valor de Mercado x Desempenho no Futebol Brasileiro (2024) - PUC-RIO

**Autor:** Rafael Akira Fukunaga
**Plataforma:** Databricks 

## Sumário
- 1. [Contexto de Negócios e Perguntas](#contexto-de-negócios-e-perguntas-etapa-2-e-41)
- 2. [Carga dos Dados](#carga-dos-dados-etapa-42)
- 3. [Modelagem e Catálogo de Dados](#modelagem-e-catálogo-de-dados-etapa-43)
- 4. [Pipeline de Dados ](#pipeline-de-dados-etapa-44)
- 5. [Qualidade de Dados](#qualidade-de-dados-etapa-45)
- 6. [Análise de Dados](#análise-de-dados-etapa-45)
- 7. [Autoavaliação](#autoavaliação)

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

Os dados foram obtidos por meio de um dataset público do Kaggle — [PREENCHER: Sofascore and Transfermarkt Football 2024 (https://www.kaggle.com/datasets/felipesembay/sofascore-and-transfermarkt-football-data), que compila informações originalmente extraídas (via scraping) do Transfermarkt e do Sofascore. O dataset chegou como 6 arquivos CSV, descritos abaixo.

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
---

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

## Pipeline de Dados (Etapa 4.4)

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

- Notebooks: [PREENCHER: links para `01_bronze_ingestion`, `02_silver_modelagem`, `03_gold_modelagem` no GitHub]
- Screenshots: [PREENCHER: screenshot do Catalog Explorer mostrando as tabelas persistidas em cada schema (bronze/silver/gold) com contagem de linhas]

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
| Jogador — resolução final (nome + clube) | [PREENCHER: rode `SELECT status_match, count(*) FROM silver.de_para_jogador_resolvido GROUP BY status_match` e preencha os números finais] |
| Time — casamento automático por sobreposição de palavras | 90 / 95 (94,7%) |
| Time — sem correspondência na fonte | 5 / 95 (5,3%) — Fortaleza, CRB, Belgrano, Caracas F.C., Águia de Marabá |

A decisão de documentar explicitamente os casos não identificados (em vez de descartá-los silenciosamente ou forçar um casamento incorreto) segue a orientação do próprio enunciado do trabalho: nem todas as perguntas precisam ser respondidas com 100% dos dados, desde que as limitações estejam claras.

---

## Análise de Dados (Etapa 4.5)

### Pergunta 1 — Valor de mercado × rating médio

```sql
-- ver query completa no notebook 03_gold_modelagem
```

**Resultado:** correlação = **0,28** (fraca), amostra de 1.158 jogadores (mínimo 5 partidas com rating registrado).

**Discussão:** a correlação é positiva e estatisticamente robusta dado o tamanho da amostra, mas fraca — r² ≈ 0,08, ou seja, o valor de mercado explica apenas ~8% da variação no rating médio em campo. Isso sugere que o mercado precifica o jogador considerando muito mais do que o desempenho estatístico de uma única temporada (potencial, idade, reputação, exposição em outras ligas).

### Pergunta 2 — Valor de mercado × participação em gols por 90 min

**Resultado:** correlação = **0,13** (muito fraca), amostra de 817 jogadores (mínimo 450 minutos jogados).

**Discussão:** ainda mais fraca que a Pergunta 1. O mercado não parece pagar prioritariamente por artilharia — zagueiros, volantes e goleiros valiosos raramente participam diretamente de gols, mas seguem caros por outras qualidades (marcação, construção de jogo, liderança) não capturadas por essa métrica.

### Pergunta 3 — Valor do elenco × desempenho coletivo do time

**Resultado (83 clubes):**

| Relação | Correlação |
|---|---|
| Valor do elenco × posse de bola média | 0,41 (moderada) |
| Valor do elenco × xG médio | 0,36 (moderada) |
| Valor do elenco × pontos por jogo | 0,47 (moderada) |

**Discussão:** este é o achado mais forte do trabalho. Agregado por elenco, o valor de mercado se torna um preditor consideravelmente mais forte de desempenho do que no nível individual (Perguntas 1 e 2) — praticamente o dobro da força de correlação. Isso sugere que, embora o valor de um jogador isolado seja um preditor ruidoso do seu desempenho pessoal, o ruído se cancela no agregado e sobra um sinal real sobre a qualidade técnica geral do grupo.

### Pergunta 4 — Idade × valor de mercado e desempenho

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

**Discussão:** a faixa de 15–17 anos (apenas 4 jogadores) é ruído estatístico e foi desconsiderada na interpretação. A partir dos 18 anos, o padrão é claro: o valor de mercado cai de forma quase monotônica com a idade, enquanto o rating médio permanece praticamente estável (entre 6,77 e 6,95) em todas as faixas. Isso responde diretamente à pergunta: existe uma janela de melhor custo-benefício, e ela é a faixa de **27 a 33 anos**, onde o rating está no pico da amostra mas o valor já caiu de forma expressiva em relação ao pico de 18–21 anos (que provavelmente reflete precificação de potencial futuro/revenda, não desempenho atual).

### Pergunta 5 — Minutos jogados como elo entre valor e desempenho

**Resultado:** correlação valor × minutos totais = **0,29** (fraca-moderada); correlação minutos × rating médio = **0,41** (moderada), amostra de 1.824 jogadores.

**Discussão:** curiosamente, quantos minutos um jogador acumula prediz melhor o seu rating (0,41) do que o valor de mercado prediz sua minutagem (0,29). Isso sugere que a titularidade está mais ligada a ritmo de jogo e confiança do técnico do que diretamente ao preço do jogador.

### Discussão geral

O padrão que conecta as 5 perguntas: **no nível do jogador individual, valor de mercado é sistematicamente um preditor fraco de desempenho estatístico** (correlações entre 0,13 e 0,29). **Agregado por elenco, porém, ele se torna um preditor moderado de desempenho coletivo** (0,36 a 0,47). A leitura mais plausível é que o mercado precifica cada jogador por fatores que vão além do rendimento de uma única temporada — reforçado pelo achado da Pergunta 4, onde jogadores mais jovens (com rating equivalente aos mais velhos) custam sistematicamente mais, provavelmente por potencial de revenda — mas que, no agregado do elenco, esse "ruído" individual se cancela e sobra um sinal real sobre a força técnica geral do time.

---

## Autoavaliação

[PREENCHER — escreva na sua própria voz. Alguns pontos que o enunciado pede que sejam discutidos:]

- **Objetivos atingidos:** as 5 perguntas formuladas na Etapa 2 foram todas respondidas com dados reais; nenhuma precisou ser removida do escopo original.
- **Dificuldades encontradas:** [PREENCHER — por exemplo, o maior desafio técnico do trabalho foi a ausência de uma chave comum entre as duas fontes de dados (Transfermarkt por ID, Sofascore por nome), que exigiu construir um processo de resolução de identidade em duas camadas (nome normalizado + desambiguação por clube) em vez de um simples JOIN.]
- **Limitações conhecidas:** cerca de [PREENCHER: percentual final] dos jogadores do Sofascore não puderam ser vinculados ao Transfermarkt (nomes/apelidos que não batem, jogadores fora do elenco atual raspado, clubes estrangeiros não cobertos pela fonte); 5 clubes não foram encontrados na fonte Transfermarkt.
- **Trabalhos futuros:** [PREENCHER — por exemplo: aplicar fuzzy matching supervisionado com revisão manual para recuperar parte dos ~33% de jogadores não identificados; expandir a análise para mais temporadas históricas; incorporar dados de posição tática (não só posição geral) para refinar a comparação de desempenho por função.]
