from gspread import Spreadsheet
from django.core.exceptions import ValidationError

from apps.roulette.models import GoogleSheet
from plugins.google.sheets import SheetsAPIClient


class RouletteGoogleAPIClient(SheetsAPIClient):

    def connect(self, key: str) -> Spreadsheet:
        return self.client.open_by_key(key)
