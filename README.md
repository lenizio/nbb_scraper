# NBB Scraper 🏀

Este projeto é um **web scraper** construído com **Scrapy** para coletar dados de jogos, times, jogadores e arremessos da liga de basquete brasileira, **NBB (Novo Basquete Brasil)**. Os dados são persistidos em um banco de dados PostgreSQL.

---

## 🛠️ Ferramentas Utilizadas

* **Scrapy**: Framework principal para web scraping.
* **Python**: Linguagem de programação utilizada.
* **PostgreSQL**: Banco de dados relacional para armazenamento dos dados extraídos.
* **Docker**: Para execução isolada e replicável do scraper.
* **Git/GitHub**: Controle de versão e hospedagem do código.
* **AWS**: Infraestrutura de nuvem para hospedar o banco de dados e gerenciar o scraping em produção.

---

## ☁️ Arquitetura de Produção na AWS

O deployment foi desenhado com foco em **segurança e escalabilidade**:

* **EC2 Instance**: Bastion host em sub-rede pública, permitindo conexão SSH segura apenas de IPs autorizados.
* **PostgreSQL RDS**: Banco de dados em sub-rede privada, inacessível diretamente da internet.
* **CloudWatch**: Captura e monitora logs do container Docker em tempo real.

Essa separação entre scraper e banco garante que o banco de dados permaneça seguro e protegido.

---

## 💾 Variáveis Coletadas

O scraper coleta dados e preenche as seguintes tabelas:

### **teams**

* `id`: Identificador único da equipe.
* `name`: Nome do time.
* `logo`: URL do logo do time.

### **players**

* `id`: Identificador único do jogador.
* `player_name`: Nome do jogador.
* `player_icon_url`: URL da foto ou ícone do jogador.

### **player\_teams\_by\_season**

* `player_id`: Referência ao jogador.
* `player_team_id`: Referência ao time.
* `season`: Temporada correspondente.
* `player_number`: Número da camisa do jogador naquela temporada.

### **games**

* `id`: Identificador único do jogo.
* `game_date`: Data do jogo.
* `game_time`: Horário do jogo.
* `home_team_id`: ID do time da casa.
* `away_team_id`: ID do time visitante.
* `home_team_score`: Pontuação do time da casa.
* `away_team_score`: Pontuação do time visitante.
* `round`: Rodada da partida.
* `stage`: Fase do campeonato.
* `season`: Temporada do jogo.
* `arena`: Local do jogo.
* `link`: Link para a página do jogo.

### **shots**

* `player_id`: Referência ao jogador que realizou o arremesso.
* `game_id`: Referência ao jogo.
* `team_id`: Referência ao time do jogador.
* `shot_quarter`: Quarto do jogo em que ocorreu o arremesso.
* `shot_time`: Tempo do arremesso no quarto.
* `shot_type`: Tipo de arremesso (ex: 2pts, 3pts).
* `shot_x_location`: Posição X do arremesso na quadra.
* `shot_y_location`: Posição Y do arremesso na quadra.

---

## 🚀 Como Replicar o Projeto Localmente

Você pode executar o scraper usando **Docker** e um banco de dados PostgreSQL local ou remoto.

### Pré-requisitos

* Docker instalado.
* Banco de dados PostgreSQL rodando localmente ou na AWS RDS.

### Passo 1: Clone o Repositório

```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio
```

### Passo 2: Construa a Imagem Docker

```bash
docker build -t nomedaimagem:tag .
```

### Passo 3: Execute o Container

```bash
docker run \
-e TEMPORADA='2023/2024' \
-e DB_HOST=host.docker.internal \
-e DB_USER=seu_usuario \
-e DB_NAME=seu_banco \
-e DB_PASS=sua_senha \
--name nomedocontainer \
nomedaimagem:tag
```

* `TEMPORADA`: Temporada a ser raspada (ex: `2019/2020`, `2020/2021`, etc.).
* `DB_HOST`, `DB_USER`, `DB_NAME`, `DB_PASS`: Conexão com PostgreSQL local ou remoto.

Os logs serão exibidos no terminal e os dados serão persistidos no banco de dados.

---

## ⚙️ Workflows de GitHub Actions 🚀

Para automatizar a integração contínua (CI) e a execução do scraper, foram implementados dois workflows principais no GitHub Actions:

### 1. docker-build-push

Este workflow é responsável por **construir a imagem Docker** do projeto e enviá-la para o Docker Hub.

* **Gatilhos (Triggers)**:
    * Automaticamente em todo **push** para a branch `main`.
    * Manualmente através de **workflow_dispatch**.
* **Ações Principais**: Faz login no Docker Hub e envia a imagem com a tag `leniziomo/scraper:latest`.

### 2. docker-run

Este workflow é responsável por **executar o scraper** em um container Docker, utilizando a imagem mais recente.

* **Gatilhos (Triggers)**:
    * Em **agendamento diário** (cron: `"0 0 * * *"` - tipicamente à meia-noite UTC).
    * Manualmente através de **workflow_dispatch**.
* **Ações Principais**:
    * Puxa a imagem `leniziomo/scraper:latest`.
    * Executa o container, passando variáveis de ambiente (como `TEMPORADA='2025/2026'`) e segredos para a conexão com o banco de dados.
    * Exporta os **logs de execução** como um artefato (`scraper-log`) para auditoria.

---
