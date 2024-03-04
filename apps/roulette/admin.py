from django.contrib import admin
from django.utils.safestring import mark_safe

from .models import Roulette, Group, Bonus, GoogleSheet


class BonusInline(admin.TabularInline):
    model = Roulette.bonus.through
    extra = 0
    show_change_link = True


@admin.register(Roulette)
class RouletteAdmin(admin.ModelAdmin):
    inlines = [
        BonusInline
    ]
    list_display = ('name', 'uuid', 'bonus_list', 'google_spreadsheet', 'updated')
    list_display_links = ('name',)
    ordering = ['name']
    exclude = ('bonus',)

    def bonus_list(self, instance) -> str:
        bonuses = instance.bonus.all()
        return mark_safe('<br>'.join(
            [
                f'<a href="/admin/roulette/bonus/{bonus.id}/change/">{bonus.title_short}</a>'
                for bonus in bonuses
            ]
        ))

    def google_spreadsheet(self, instance) -> str:
        return mark_safe(f'<a href="https://docs.google.com/spreadsheets/d/{instance.google_table.key}/" target="_blank">{instance.google_table.title}</a>')

    bonus_list.short_description = "Список бонусов"


@admin.register(Bonus)
class BonusAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'price', 'group', 'probability')
    list_display_links = ('id', 'name')
    # list_editable = ('group', 'price', 'probability')


@admin.register(Group)
class GroupAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'probability')
    list_display_links = ('id', 'name')
    # list_editable = ('probability',)


@admin.register(GoogleSheet)
class GoogleSheetAnalytic(admin.ModelAdmin):
    list_display = ("id", "link_title", "key")
    search_fields = ("name", "title", "key")

    def link_title(self, instance: GoogleSheet) -> str:
        return mark_safe(
            '<a href="https://docs.google.com/spreadsheets/d/%(key)s/" '
            'target="_blank">%(title)s</a>'
            % {"key": instance.key, "title": instance.title}
        )

    link_title.short_description = "Заголовок"
