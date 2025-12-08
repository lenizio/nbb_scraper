import psycopg2
from psycopg2 import sql
from psycopg2.errors import UniqueViolation, NotNullViolation, InFailedSqlTransaction
from itemadapter import ItemAdapter
import hashlib
import logging
import os
from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import OperationalError
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

try:
    DB_CONFIG = {
        'host': os.environ['DB_HOST'],
        'database': os.environ['DB_NAME'],
        'user': os.environ['DB_USER'],
        'password': os.environ['DB_PASS'],
        'port': os.environ.get('DB_PORT', '5432')  
    }
except KeyError as e:
    logger.critical(f"Variável de ambiente obrigatória ausente: {e}")
    raise SystemExit(f"Variável de ambiente obrigatória ausente: {e}")


try:
    POOL = ThreadedConnectionPool(minconn=1, maxconn=32, **DB_CONFIG)
    logger.info("Pool de conexões criado com sucesso.")
except OperationalError as e:
    logger.critical(f"Erro ao criar pool de conexões: {e}", exc_info=True)
    raise SystemExit(f"Não foi possível criar o pool de conexões: {e}")

def close_pool():
    """Fecha todas as conexões do pool."""
    if POOL:
        POOL.closeall()
        logger.info("Pool de conexões fechado.")


class DatabaseManager:
    """Gerencia uma única conexão e cursor de banco de dados para múltiplas operações."""
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.cur = None

    def __enter__(self):
        """Estabelece uma conexão e retorna o cursor ao entrar em um bloco 'with'."""
        try:
            self.conn = POOL.getconn()
            self.conn.autocommit = False
            self.cur = self.conn.cursor()
            return self
        except Exception as e:
            logger.error(f"Erro ao obter conexão do pool: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fecha o cursor e a conexão, confirmando ou desfazendo as alterações com base no sucesso."""
        if self.cur:
            self.cur.close()
        if self.conn:
            if exc_type is None: 
                self.conn.commit()
            else: 
                self.conn.rollback()
                logger.error(f"Transação revertida devido a uma exceção: {exc_val}", exc_info=True)
            POOL.putconn(self.conn)

    def create_tables(self):
        """
        Cria todas as tabelas necessárias se elas ainda não existirem.
        Este método é chamado uma vez em uma instância do DatabaseManager.
        """
        try:
            self.cur.execute("""
                -- Table for teams (id is PRIMARY KEY, automatically indexed)
                CREATE TABLE IF NOT EXISTS teams_table (
                    id VARCHAR(100) PRIMARY KEY,
                    name VARCHAR(50),
                    logo TEXT
                );
                
                CREATE TABLE IF NOT EXISTS players_table (
                    id INTEGER PRIMARY KEY,
                    player_name VARCHAR(50),
                    player_icon_url TEXT
                );

                -- Table for player's team per season (composite PRIMARY KEY, automatically indexed)
                CREATE TABLE IF NOT EXISTS player_teams_by_season_table (
                    player_id INTEGER REFERENCES players_table(id) NOT NULL,
                    player_team_id VARCHAR(100) REFERENCES teams_table(id) NOT NULL,
                    season VARCHAR(20) NOT NULL,
                    player_number VARCHAR(10),
                    PRIMARY KEY (player_id, player_team_id, season)
                );
                
                -- Table for games (game_id is PRIMARY KEY, automatically indexed)
                CREATE TABLE IF NOT EXISTS games_table (
                    id INTEGER PRIMARY KEY,
                    game_date DATE,
                    game_time TIME,
                    home_team_id VARCHAR(100) REFERENCES teams_table(id),
                    away_team_id VARCHAR(100) REFERENCES teams_table(id),
                    home_team_score INTEGER,
                    away_team_score INTEGER,
                    round VARCHAR(30),
                    stage VARCHAR(30),
                    season VARCHAR(20),
                    arena VARCHAR(100),
                    link TEXT
                );

                -- Table for shots 
                CREATE TABLE IF NOT EXISTS shots_table (
                    id SERIAL PRIMARY KEY,
                    player_id INTEGER REFERENCES players_table(id),
                    game_id INTEGER REFERENCES games_table(id),
                    team_id VARCHAR(100) REFERENCES teams_table(id),
                    shot_quarter VARCHAR(10),
                    shot_time TIME,
                    shot_type VARCHAR(20),
                    shot_x_location FLOAT,
                    shot_y_location FLOAT
                );
            """)
            logger.info("Tabelas criadas ou já existentes.")
        except Exception as e:
            logger.error(f"Erro ao criar tabelas: {e}", exc_info=True)
            self.conn.rollback() 
            raise 
            
    def get_id_games_by_season(self,season):
        """
        CORREÇÃO: Referência a uma tabela `player_stats_table` não definida
        e alias incorreto para `games_table`.
        Assumindo que a lista de jogos por temporada deve vir da `games_table`.
        Se você tiver uma tabela `player_stats_table` real, ajuste a consulta.
        """
        try:
           
            self.cur.execute(
                """
                SELECT DISTINCT g.id
                FROM games_table g
                WHERE g.season = %s;
                """,
                (season,)
            )
            results = self.cur.fetchall()
            return [row[0] for row in results]
        except Exception as e:
            logger.error(f"Erro ao selecionar jogos {e}", exc_info=True)
            self.conn.rollback()
            raise
    def insert_team(self, team_item):
        try:
            adapter = ItemAdapter(team_item)
            team_id = adapter.get('id')
            name = adapter.get('name')
            logo = adapter.get('logo')

            if not team_id:
                logger.warning(f"Tentativa de inserir equipe sem ID. Dados: {team_item}")
                return

            self.cur.execute(
                """
                INSERT INTO teams_table (id, name, logo)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (team_id, name, logo)
            )
        except (NotNullViolation, InFailedSqlTransaction, psycopg2.Error) as e:
            self.conn.rollback()
            logger.error(f"Erro ao inserir/atualizar equipe '{team_id}': {e}", exc_info=True)
            raise
    
    def insert_player(self, player_item):
        try:
            adapter = ItemAdapter(player_item)
            player_id = adapter.get('player_id')
            player_name = adapter.get('player_name')
            player_icon_url = adapter.get('player_icon_url')

            if not player_id:
                logger.warning(f"Tentativa de inserir jogador sem ID. Dados: {player_item}")
                return

            self.cur.execute(
                """
                INSERT INTO players_table (id, player_name, player_icon_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (player_id, player_name, player_icon_url)
            )
        except (NotNullViolation, InFailedSqlTransaction, psycopg2.Error) as e:
            self.conn.rollback()
            logger.error(f"Erro ao inserir/atualizar jogador '{player_id}': {e}", exc_info=True)
            raise
    
    def insert_player_team_by_season(self, player_item):
        try:
            adapter = ItemAdapter(player_item)
            player_id = adapter.get('player_id')
            player_team_id = adapter.get('player_team_id')
            season = adapter.get('season')
            player_number = adapter.get('player_number')

            if not all([player_id, player_team_id, season]):
                logger.warning(f"Dados faltando para player_teams_by_season. Dados: {player_item}")
                return

            self.cur.execute(
                """
                INSERT INTO player_teams_by_season_table (player_id, player_team_id, season, player_number)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (player_id, player_team_id, season) DO NOTHING;
                """,
                (player_id, player_team_id, season, player_number)
            )
        except (NotNullViolation, InFailedSqlTransaction, psycopg2.Error) as e:
            self.conn.rollback()
            logger.error(f"Erro ao inserir time do jogador '{player_id}' na temporada '{season}': {e}", exc_info=True)
            raise

    def insert_game(self, game_item):
        try:
            adapter = ItemAdapter(game_item)
            game_id = adapter.get('game_id')

            if not game_id:
                logger.warning(f"Tentativa de inserir jogo sem ID. Dados: {game_item}")
                return

            self.cur.execute(
                """
                INSERT INTO games_table (
                    id, game_date, game_time,
                    home_team_id, away_team_id,
                    home_team_score, away_team_score,
                    round, stage, season, arena, link
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET game_date = EXCLUDED.game_date,
                    game_time = EXCLUDED.game_time,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    home_team_score = EXCLUDED.home_team_score,
                    away_team_score = EXCLUDED.away_team_score,
                    round = EXCLUDED.round,
                    stage = EXCLUDED.stage,
                    season = EXCLUDED.season,
                    arena = EXCLUDED.arena,
                    link = EXCLUDED.link;
                """,
                (
                    game_id,
                    adapter.get('game_date'),
                    adapter.get('game_time'),
                    adapter.get('home_team_id'),
                    adapter.get('away_team_id'),
                    adapter.get('home_team_score'),
                    adapter.get('away_team_score'),
                    adapter.get('round'),
                    adapter.get('stage'),
                    adapter.get('season'),
                    adapter.get('arena'),
                    adapter.get('link')
                )
            )
        except (NotNullViolation, InFailedSqlTransaction, psycopg2.Error) as e:
            self.conn.rollback()
            logger.error(f"Erro ao inserir/atualizar jogo '{game_id}': {e}", exc_info=True)
            raise

    
    def insert_shot(self, shot_item):
        """
        Nenhuma alteração na lógica de inserção necessária aqui, apenas no fluxo de commit.
        """
        try:
            adapter = ItemAdapter(shot_item)
            player_id = adapter.get('player_id')
            game_id = adapter.get('game_id')
            team_id = adapter.get('team_id')

            shot_quarter = adapter.get('shot_quarter')
            shot_time = adapter.get('shot_time')
            shot_type = adapter.get('shot_type')
            shot_x_location = adapter.get('shot_x_location')
            shot_y_location = adapter.get('shot_y_location')

            if not all([player_id, game_id, team_id]):
                logger.warning(
                    f"Campos essenciais ausentes em shot: {shot_item}"
                )
                return

            self.cur.execute(
                """
                INSERT INTO shots_table (
                    player_id, game_id, team_id,
                    shot_quarter, shot_time, shot_type,
                    shot_x_location, shot_y_location
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    player_id, game_id, team_id,
                    shot_quarter, shot_time, shot_type,
                    shot_x_location, shot_y_location
                )
            )
            

        except Exception as e:
            self.conn.rollback()
            logger.error(
                f"Erro ao inserir arremesso do jogador {player_id} no jogo {game_id}: {e}",
                exc_info=True
            )
            raise

    
    def close(self):
        self.cur.close()
        self.conn.close()



if __name__ == "__main__":
    print("Tentando criar tabelas do banco de dados...")
    with DatabaseManager(DB_CONFIG) as db_manager_setup:
        db_manager_setup.create_tables()
    print("Configuração do banco de dados completa (se nenhum erro ocorreu). Verifique db_inserts.log para detalhes.")
