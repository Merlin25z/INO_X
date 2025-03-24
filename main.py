import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os
from datetime import datetime

# =============================================
# 1. НАСТРОЙКИ (ИЗМЕНИТЕ ПОД СВОЮ СИСТЕМУ)
# =============================================
PG_USER = "postgres"
PG_PASSWORD = "2589"  # Замените на реальный пароль
PG_DB = "sakila"
SQLITE_DB_PATH = "sqlite-sakila.db"  # Путь к файлу SQLite


# =============================================
# 2. ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗАМИ ДАННЫХ
# =============================================
def load_sqlite_to_postgres():
    """Загружает все таблицы из SQLite в PostgreSQL"""
    # Проверяем наличие SQLite-файла
    if not os.path.exists(SQLITE_DB_PATH):
        raise FileNotFoundError(f"Файл {SQLITE_DB_PATH} не найден!")

    # Создаем папки для результатов
    os.makedirs("results", exist_ok=True)
    os.makedirs("plots", exist_ok=True)

    # Подключаемся к SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"Найдены таблицы в SQLite: {tables}")

    # Подключаемся к PostgreSQL
    engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@localhost:5432/{PG_DB}')

    # Переносим данные
    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)
            df.to_sql(table, engine, if_exists='replace', index=False)
            print(f"Таблица {table} успешно загружена в PostgreSQL")
        except Exception as e:
            print(f"Ошибка при загрузке {table}: {e}")

    sqlite_conn.close()
    engine.dispose()
    return tables


# =============================================
# 3. ВЫПОЛНЕНИЕ ЗАПРОСОВ И АНАЛИЗ
# =============================================
# ... (предыдущий код остается без изменений до функции run_queries_and_analysis)

def run_queries_and_analysis():
    """Выполняет все аналитические запросы"""
    engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@localhost:5432/{PG_DB}')

    # Запрос 1: Доля фильмов по рейтингу (уже было)
    query1 = """
    SELECT 
        rating, 
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM film), 2) as percentage
    FROM film
    GROUP BY rating
    ORDER BY percentage DESC;
    """
    df1 = pd.read_sql(query1, engine)
    df1.to_csv("results/film_ratings.csv", index=False)

    # Визуализация 1
    plt.figure(figsize=(10, 5))
    df1.plot(kind='bar', x='rating', y='percentage', legend=False)
    plt.title("Доля фильмов по рейтингу")
    plt.ylabel("Процент (%)")
    plt.savefig("plots/film_ratings.png")
    plt.close()

    # Запрос 2: Популярные категории (уже было)
    query2 = """
    SELECT 
        c.name as category,
        COUNT(*) as rental_count
    FROM rental r
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film_category fc ON i.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    GROUP BY c.name
    ORDER BY rental_count DESC
    LIMIT 5;
    """
    df2 = pd.read_sql(query2, engine)
    df2.to_csv("results/popular_categories.csv", index=False)

    # Визуализация 2
    plt.figure(figsize=(10, 5))
    df2.plot(kind='barh', x='category', y='rental_count', legend=False)
    plt.title("Топ-5 популярных категорий")
    plt.xlabel("Количество аренд")
    plt.savefig("plots/popular_categories.png")
    plt.close()

    # Запрос 3: Средняя продолжительность проката по категориям
    query3 = """
    SELECT 
        c.name as category,
        AVG(f.rental_duration) as avg_rental_duration
    FROM film f
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    GROUP BY c.name
    ORDER BY avg_rental_duration DESC;
    """
    df3 = pd.read_sql(query3, engine)
    df3.to_csv("results/avg_rental_duration.csv", index=False)

    # Визуализация 3
    plt.figure(figsize=(12, 6))
    df3.plot(kind='bar', x='category', y='avg_rental_duration', legend=False)
    plt.title("Средняя продолжительность проката по категориям")
    plt.ylabel("Дней")
    plt.xticks(rotation=45)
    plt.savefig("plots/avg_rental_duration.png", bbox_inches='tight')
    plt.close()

    # Запрос 4: Ежемесячный доход за последний год (ИСПРАВЛЕННАЯ ВЕРСИЯ)
    query4 = """
    SELECT 
        DATE_TRUNC('month', p.payment_date::timestamp) as month,
        SUM(p.amount) as monthly_revenue
    FROM payment p
    WHERE p.payment_date::timestamp >= (SELECT MAX(payment_date::timestamp) - INTERVAL '1 year' FROM payment)
    GROUP BY DATE_TRUNC('month', p.payment_date::timestamp)
    ORDER BY month;
    """
    df4 = pd.read_sql(query4, engine)
    df4['month'] = pd.to_datetime(df4['month'])
    df4.to_csv("results/monthly_revenue.csv", index=False)

    # Визуализация 4
    plt.figure(figsize=(12, 6))
    plt.plot(df4['month'], df4['monthly_revenue'], marker='o')
    plt.title("Ежемесячный доход за последний год")
    plt.ylabel("Доход ($)")
    plt.xlabel("Месяц")
    plt.grid(True)
    plt.savefig("plots/monthly_revenue.png")
    plt.close()

    # Запрос 5: Сравнение продаж по магазинам
    query5 = """
    SELECT 
        s.store_id,
        a.address,
        SUM(p.amount) as total_sales
    FROM store s
    JOIN staff st ON s.manager_staff_id = st.staff_id
    JOIN payment p ON st.staff_id = p.staff_id
    JOIN address a ON s.address_id = a.address_id
    GROUP BY s.store_id, a.address
    ORDER BY total_sales DESC;
    """
    df5 = pd.read_sql(query5, engine)
    df5.to_csv("results/store_sales.csv", index=False)

    # Визуализация 5
    plt.figure(figsize=(10, 5))
    df5.plot(kind='bar', x='address', y='total_sales', legend=False)
    plt.title("Сравнение продаж по магазинам")
    plt.ylabel("Общий доход ($)")
    plt.xticks(rotation=15)
    plt.savefig("plots/store_sales.png", bbox_inches='tight')
    plt.close()

    # Запрос 6: Средние затраты на замену по жанрам
    query6 = """
    SELECT 
        c.name as category,
        AVG(f.replacement_cost) as avg_replacement_cost
    FROM film f
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    GROUP BY c.name
    ORDER BY avg_replacement_cost DESC;
    """
    df6 = pd.read_sql(query6, engine)
    df6.to_csv("results/replacement_cost.csv", index=False)

    # Визуализация 6
    plt.figure(figsize=(12, 6))
    df6.plot(kind='bar', x='category', y='avg_replacement_cost', legend=False)
    plt.title("Средние затраты на замену по категориям")
    plt.ylabel("Стоимость ($)")
    plt.xticks(rotation=45)
    plt.savefig("plots/replacement_cost.png", bbox_inches='tight')
    plt.close()

    # Запрос 7: Актеры, снимающиеся в разных жанрах
    query7 = """
    SELECT 
        a.actor_id,
        a.first_name || ' ' || a.last_name as actor_name,
        COUNT(DISTINCT fc.category_id) as unique_categories_count
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film_category fc ON fa.film_id = fc.film_id
    GROUP BY a.actor_id, actor_name
    HAVING COUNT(DISTINCT fc.category_id) > 5
    ORDER BY unique_categories_count DESC
    LIMIT 10;
    """
    df7 = pd.read_sql(query7, engine)
    df7.to_csv("results/actors_diverse_genres.csv", index=False)

    # Визуализация 7
    plt.figure(figsize=(12, 6))
    df7.plot(kind='barh', x='actor_name', y='unique_categories_count', legend=False)
    plt.title("Актеры, снимающиеся в разных жанрах (более 5)")
    plt.xlabel("Количество уникальных жанров")
    plt.savefig("plots/actors_diverse_genres.png", bbox_inches='tight')
    plt.close()

    # Статистический анализ (остается без изменений)
    merged_query = """
    SELECT 
        f.film_id, 
        MAX(f.title) as title,
        MAX(f.rating) as rating,
        c.name as category,
        MAX(f.rental_duration) as rental_duration,
        MAX(f.replacement_cost) as replacement_cost,
        COUNT(r.rental_id) as rental_count
    FROM film f
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    JOIN inventory i ON f.film_id = i.film_id
    LEFT JOIN rental r ON i.inventory_id = r.inventory_id
    GROUP BY f.film_id, c.name;
    """

    merged_df = pd.read_sql(merged_query, engine)
    merged_df.to_csv("results/merged_data.csv", index=False)

    # Анализ числовых данных
    numeric_stats = merged_df.describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9])
    numeric_stats.to_csv("results/numeric_stats.csv")

    # Анализ категориальных данных
    categorical_stats = pd.DataFrame({
        'column': ['rating', 'category'],
        'missing_values': [merged_df['rating'].isnull().mean(), merged_df['category'].isnull().mean()],
        'unique_values': [merged_df['rating'].nunique(), merged_df['category'].nunique()],
        'mode': [merged_df['rating'].mode()[0], merged_df['category'].mode()[0]]
    })
    categorical_stats.to_csv("results/categorical_stats.csv", index=False)

    engine.dispose()


# ... (остальной код остается без изменений)


# =============================================
# 4. ЗАПУСК ВСЕГО ПРОЦЕССА
# =============================================
if __name__ == "__main__":
    print("=" * 50)
    print("НАЧАЛО РАБОТЫ")
    print(f"Дата и время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50 + "\n")

    try:
        # Шаг 1: Загрузка данных
        print("[1/3] Загрузка данных из SQLite в PostgreSQL...")
        tables = load_sqlite_to_postgres()
        print(f"Успешно загружено таблиц: {len(tables)}\n")

        # Шаг 2: Выполнение запросов и анализ
        print("[2/3] Выполнение аналитических запросов...")
        run_queries_and_analysis()
        print("Анализ завершен. Результаты сохранены в папки 'results' и 'plots'\n")

        # Шаг 3: Финализация
        print("[3/3] Готово!")
        print("=" * 50)
        print("ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ УСПЕШНО")
        print("=" * 50)

    except Exception as e:
        print("\n" + "=" * 50)
        print("ПРОИЗОШЛА ОШИБКА:")
        print(str(e))
        print("=" * 50)