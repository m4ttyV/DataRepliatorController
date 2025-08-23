# DataReplicatorController

Инструмент для актуализации и сравнения данных между родительской (master) базой данных и дочерними (slave) базами PostgreSQL.  
Программа позволяет автоматически проверять совпадение данных в указанных таблицах и формировать лог с результатами.

## 🚀 Возможности
- Подключение к PostgreSQL через [Npgsql](https://www.npgsql.org/).
- Сравнение строк и колонок таблиц **построчно и поколоночно**.
- Игнорирование пространственных типов данных (`geometry`, `geography`).
- Гибкая настройка через `appsetting.json`.
- Автоматическая генерация логов:
  - `./logs/{yyyy}/{MM}/log-{dd.MM.yyyy}.txt`
  - `./logs/log-last.txt`

## 📂 Структура проекта
- `Program.cs` – основная логика (загрузка конфигурации, подключение к БД, сравнение таблиц, сохранение логов).
- `appsetting.json` – конфигурационный файл с описанием родительской и дочерних БД.
- `logs/` – папка для хранения отчётов о проверках.

## ⚙️ Настройка
Перед запуском укажите параметры подключения в `appsetting.json`:

json
{
  "ParentDatabase": {
    "ConnectionString": "Host=your_host;Port=5432;Database=parent_db;Username=user;Password=password;",
    "Schema": "Schema0"
  },
  "ChildDatabases": [
    {
      "ConnectionString": "Host=your_host;Port=5432;Database=child_db1;Username=user;Password=password;",
      "Schema": "Schema1",
      "Tables": [ "table1", "table2", "table3" ]
    }
  ]
}

Поля конфигурации
ParentDatabase – родительская база данных:

ConnectionString – строка подключения к PostgreSQL.

Schema – схема, где находятся эталонные таблицы.

ChildDatabases – список дочерних баз:

ConnectionString – строка подключения.

Schema – схема для проверки.

Tables – список таблиц для сравнения.

▶️ Запуск
Соберите проект в Visual Studio.

Убедитесь, что установлен .NET и доступен пакет Npgsql.

В папке с проектом положите appsetting.json.

Запустите программу – результат сравнения появится в ./logs.

📝 Пример лога
vbnet
Копировать
Редактировать
[OK] ParentDB:Schema0.table1-ChildDB:Schema1.table1
[Error] ParentDB:Schema0.table2-ChildDB:Schema1.table2
[Error - Error connecting to the database or table] ParentDB:Schema0.table3-ChildDB:Schema1.table3
📦 Зависимости
.NET 6+

Npgsql – драйвер для PostgreSQL

Newtonsoft.Json
