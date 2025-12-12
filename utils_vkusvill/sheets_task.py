import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
import numpy as np

def upload_csv_files_to_multiple_sheets(csv_files_with_sheets, json_keyfile_path):
    """
    Загружает CSV-файлы в отдельные Google Sheets, создавая листы по имени файла.
    Обрабатывает NaN и бесконечности, чтобы данные корректно загрузились.

    Аргументы:
        csv_files_with_sheets: Путь к CSV файлам, которые нужны для загрузки.
        json_keyfile_path: Путь к json ключу для доступа.
    """
    # Подключаемся к Google Sheets API
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)

    # Ищем файлы по указанному пути
    for csv_path, sheet_id in csv_files_with_sheets.items():
        if not os.path.isfile(csv_path):
            print(f"Файл не найден: {csv_path}")
            continue

        # Загружаем CSV
        df = pd.read_csv(csv_path)

        # Обрабатываем NaN и бесконечности
        df = df.replace({np.nan: "", np.inf: "", -np.inf: ""})

        # Имя листа по имени файла без расширения
        sheet_name = os.path.splitext(os.path.basename(csv_path))[0]

        # Открываем таблицу
        sheet = client.open_by_key(sheet_id)

        # Создаём или очищаем лист
        try:
            worksheet = sheet.worksheet(sheet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(
                title=sheet_name, rows=str(len(df)+1), cols=str(len(df.columns))
            )

        # Загружаем данные
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"Загружен CSV '{csv_path}' в таблицу '{sheet_id}', лист '{sheet_name}'")