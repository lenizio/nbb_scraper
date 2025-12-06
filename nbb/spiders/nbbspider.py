import scrapy
from nbb.db_manager import DatabaseManager, DB_CONFIG, close_pool
from nbb.items import  GameItem, ShotItem, PlayerItem, TeamItem
from nbb.item_loaders.games_loaders import GameLoader 
from nbb.item_loaders.shots_loaders import ShotLoader
from nbb.item_loaders.player_loader import PlayerLoader
from nbb.item_loaders.team_loader import TeamLoader
import hashlib
import os

urls = {
    '2017/2018': 'https://lnb.com.br/nbb/tabela-de-jogos/?season%5B%5D=41',
    '2018/2019': 'https://lnb.com.br/nbb/tabela-de-jogos/?season%5B%5D=47&wherePlaying=-1&played=-1',
    '2019/2020': 'https://lnb.com.br/nbb/tabela-de-jogos/?season%5B%5D=54',
    '2020/2021': 'https://lnb.com.br/nbb/tabela-de-jogos/?season%5B%5D=59&wherePlaying=-1&played=-1',
    '2021/2022': 'https://lnb.com.br/nbb/tabela-de-jogos/?season%5B%5D=63',
    '2022/2023': 'https://lnb.com.br/nbb/tabela-de-jogos/?season%5B%5D=71&wherePlaying=-1&played=-1',
    '2023/2024': 'https://lnb.com.br/nbb/tabela-de-jogos/?season%5B%5D=80&wherePlaying=-1&played=-1',
    '2024/2025': 'https://lnb.com.br/nbb/tabela-de-jogos/?season%5B%5D=88',
    '2025/2026': 'https://lnb.com.br/nbb/tabela-de-jogos'
}

try:
    temporada = os.environ['TEMPORADA']
except KeyError:
    opcoes = ", ".join(urls.keys())
    raise SystemExit(f"Variável de ambiente TEMPORADA não encontrada. Opções válidas: {opcoes}")

if temporada not in urls:
    opcoes = ", ".join(urls.keys())
    raise SystemExit(f"Valor inválido para TEMPORADA: '{temporada}'. Opções válidas: {opcoes}")

# Define a URL inicial
url = urls[temporada]



class GameSpider(scrapy.Spider):
    
    name = 'games'
    start_urls=[url]
    
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with DatabaseManager(DB_CONFIG) as db:
            self.existing_game_ids = db.get_id_games_by_season(temporada)

    def parse(self, response):

        games_table = response.css("table.table_matches_table tbody:nth-of-type(1) tr")
        
        for game in games_table:
            
            check = game.css('td.score_value.show-for-medium span.home::text').get()
            
            if check is None:
                
                break
            
            game_id = int(game.css('td::attr(data-real-id)').get())

            if game_id in self.existing_game_ids:
                
                continue
            
            away_team_loader = TeamLoader(item = TeamItem(), selector = game)
            home_team_loader = TeamLoader(item = TeamItem(), selector = game)
            away_team_loader.add_css('name','td.visitor_team_value.show-for-medium span.team-shortname::text')
            away_team_loader.add_css('logo', 'td.logo_visitor_team.show-for-medium img::attr(src)')
            home_team_loader.add_css('name','td.home_team_value.show-for-medium span.team-shortname::text')
            home_team_loader.add_css('logo', 'td.logo_home_team.show-for-medium img::attr(src)')
            
            away_team_item = away_team_loader.load_item()
            home_team_item = home_team_loader.load_item()
            yield away_team_item
            yield home_team_item
            
            
            away_team_id = self.generate_team_id(away_team_item.get('logo'))
            home_team_id = self.generate_team_id(home_team_item.get('logo') )
            
            game_loader = GameLoader(item = GameItem(), selector = game)
            
            game_loader.add_css('game_id', 'td::attr(data-real-id)')
            game_loader.add_value('home_team_id',home_team_id)
            game_loader.add_value('away_team_id',away_team_id)
            game_loader.add_css('game_date', 'td.date_value.show-for-medium span::text')
            game_loader.add_css('game_time','td.date_value.show-for-medium span:nth-child(2)::text')
            game_loader.add_css('home_team_score','td.score_value.show-for-medium span.home::text')
            game_loader.add_css('away_team_score','td.score_value.show-for-medium span.away::text')
            game_loader.add_css('round','td.game_value.hide_value span::text')
            game_loader.add_css('stage','td.stage_value.hide_value::text')
            game_loader.add_css('season','td.champ_value.hide_value::text')
            game_loader.add_css('arena', 'td.gym_value.hide_value::text')
            game_loader.add_css('link','td.score_value a.match_score_relatorio::attr(href)')
            
            game_item = game_loader.load_item()

            yield game_item

            game_link = game_item.get('link')
            game_id = game_item.get('game_id')
            season = game_item.get('season')
            
            if game_link:
                yield response.follow(
                    game_link,
                    self.parse_athlete,
                    meta={'game_id': game_id, 'season': season, 'home_team_id': home_team_id,'away_team_id': away_team_id, 'game_link': game_link}
                )         
            
    def parse_athlete(self, response):
        season = response.meta['season']
        home_team_id = response.meta['home_team_id']
        away_team_id = response.meta['away_team_id']

        players_info = {"home": {}, "away": {}}

        teams = [
            ("away", away_team_id, response.css("div.graphic_move div.players_block.players_block_right li")),
            ("home", home_team_id, response.css("div.graphic_move div.players_block.players_block_left li")),
        ]

        for side, team_id, players in teams:
            for player in players:
                player_loader = PlayerLoader(item=PlayerItem(), selector=player)
                player_loader.add_css("player_number", "div.number::text")
                player_loader.add_css("player_name", "div.name::text")
                player_loader.add_css("player_id", "::attr(idj)")
                player_loader.add_css("player_photo", "::attr(avatar)")
                player_loader.add_value("player_team_id", team_id)
                player_loader.add_value("season", season)

                player_item = player_loader.load_item()
                yield player_item

                player_id = player_item.get("player_id")
                player_name = player_item.get("player_name")

                players_info[side][player_id] = player_name

        # Se precisar usar players_info depois
        yield from self.parse_shots(response)   
    
    def parse_shots(self,response):
        game_id = response.meta['game_id']
        home_team_id = response.meta['home_team_id']
        away_team_id = response.meta['away_team_id']
        shots = response.css('div.graphic_gym li')
        
        for shot in shots:
            shot_loader = ShotLoader(item = ShotItem(), selector = shot)
            
            id_team =shot.css('::attr(ide)').get()
            shot_loader.add_css('player_id','::attr(idj)')
            shot_loader.add_css('shot_quarter','::attr(idp)')
            shot_loader.add_css('shot_type','::attr(class)')
            shot_loader.add_css('shot_time','::attr(time)')
            shot_loader.add_css('shot_x_location','::attr(style)')
            shot_loader.add_css('shot_y_location','::attr(style)')
            shot_loader.add_value('game_id', game_id)
            
            if id_team == '1':
                shot_loader.add_value('team_id',home_team_id)
            elif id_team == '2':
                shot_loader.add_value('team_id',away_team_id)
            
            yield shot_loader.load_item()
         
    
    def transform_quarter(self,value):
    
        if str(value).lower() == 'general':
            return 'Total'
        
        try:
            valor_int = int(value)
            return valor_int
        except ValueError:
            pass
        return None 
    
    
    def generate_team_id(self, logo_url):
        return hashlib.md5(logo_url.encode('utf-8')).hexdigest()
