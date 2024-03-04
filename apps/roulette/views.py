import logging
from collections import defaultdict

import gspread
from rest_framework.permissions import AllowAny
from django.http import JsonResponse
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404
from random import choices
from rest_framework.response import Response
import datetime

from .models import Roulette

from .utils import RouletteGoogleAPIClient as GoogleAPIClient


class RouletteView(APIView):
    permission_classes = [AllowAny, ]
    trying_action_to_google: int = 5

    @staticmethod
    def __get_bonus(request) -> dict:

        uuid = request.GET.get('uuid')
        roulette = get_object_or_404(Roulette, uuid=uuid)

        # все бонусы для данного объекта Roulette
        bonuses = roulette.bonus.all()

        # количество бонусов и их вероятности
        bonus_counts = [1, 2, 3]
        probabilities = [0.5, 0.3, 0.2]

        # Выберите количество бонусов с учетом вероятностей
        # т.е сколько выпадет бонусов для данной рулетки
        num_bonuses = choices(bonus_counts, probabilities)[0]

        # словарь, где каждому типу бонуса соответствует
        # список всех бонусов этого типа
        bonus_groups = defaultdict(list)
        for bonus in bonuses:
            bonus_groups[bonus.group].append(bonus)

        # случайный тип бонусов для каждого бонуса
        selected_bonuses = []
        for i in range(num_bonuses):
            if not bonus_groups:  # Если все группы бонусов уже использованы, прерываем цикл
                break

            # Выбираем случайную группу в соответствии с вероятностью
            group_probabilities = [group.probability for group in bonus_groups.keys()]
            current_group = choices(
                list(bonus_groups.keys()),
                group_probabilities
            )[0]

            # Выбираем случайный бонус для данной группы в соответствии с вероятностью
            group_bonuses = bonus_groups.pop(current_group)
            bonus_probabilities = [bonus.probability for bonus in group_bonuses]
            selected_bonus = choices(
                group_bonuses,
                bonus_probabilities
            )[0]

            selected_bonuses.append(selected_bonus)

        response = {'bonus': {
            bonus.name: bonus.price for bonus in selected_bonuses
        }
        }
        response.update({'uuid': uuid})
        return response

    def get(self, request):
        bonus = self.__get_bonus(request)
        request.session['bonus_uuid'] = bonus
        request.session.save()
        return JsonResponse(bonus)

    def post(self, request):
        response = request.session.get('bonus_uuid', {})
        uuid = response.get('uuid')
        roulette = get_object_or_404(Roulette, uuid=uuid)
        roulette_main = get_object_or_404(Roulette, uuid='c0ec97ebbd2b46a4b0a2')
        main_table_key = roulette_main.google_table.key
        spreadsheet_key = roulette.google_table.key
        google_client = GoogleAPIClient()

        for i in range(self.trying_action_to_google):
            try:
                worksheet = google_client.connect(spreadsheet_key).worksheet("bonus")
                worksheet_main = google_client.connect(main_table_key).worksheet("bonus")
                break
            except gspread.exceptions.SpreadsheetNotFound:
                logging.debug('SpreadsheetNotFound')
                continue
            except gspread.exceptions.WorksheetNotFound:
                logging.debug(f'Нет листа "Bonus" в таблице')
                continue
            except Exception as err:
                logging.error(err)
                continue
        else:
            return JsonResponse({'error': 'SpreadsheetError'}, status=400)

        # данные для отправки в google таблицу
        bonuses = response.get('bonus')
        bonus = "".join(
            [f"\n• {k} ({v} руб)" for k, v in bonuses.items()]
        )[1:]
        name = request.POST.get('name')
        phone = request.POST.get('phone')
        email = request.POST.get('email')
        # level = roulette.name.split()[0]
        date_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = [date_now, name, email, phone, bonus]
        data_2_main = [date_now, name, email, phone, bonus, roulette.name]
        for i in range(self.trying_action_to_google):
            try:
                worksheet.append_row(data)
                worksheet_main.append_row(data_2_main)
                return Response({"status": "ok"})
            except Exception as err:
                logging.error(err)
                continue

        return JsonResponse({'error': 'SpreadsheetError'}, status=400)
