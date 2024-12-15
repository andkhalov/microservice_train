import time
import pandas as pd
import matplotlib.pyplot as plt
import os

# Пути к логам и файлу изображения
LOG_FILE = '/usr/src/app/logs/metric_log.csv'
PLOT_FILE = '/usr/src/app/logs/error_distribution.png'

# Интервал обновления гистограммы в секундах
REFRESH_INTERVAL = 10

def plot_histogram():
    """
    Читает метрики из LOG_FILE и строит гистограмму абсолютных ошибок.
    """
    if os.path.exists(LOG_FILE):
        try:
            # Загружаем данные из CSV
            df = pd.read_csv(LOG_FILE)
            print("Содержимое metric_log.csv:")
            print(df)

            if not df.empty:
                print("Данные успешно загружены. Строим гистограмму...")
                # Строим гистограмму по абсолютной ошибке
                plt.figure(figsize=(8, 6))
                plt.hist(df['absolute_error'], bins=30, edgecolor='black', alpha=0.75)
                plt.title("Распределение абсолютных ошибок")
                plt.xlabel("Абсолютная ошибка")
                plt.ylabel("Частота")
                plt.grid(axis='y', linestyle='--', linewidth=0.7)

                # Сохраняем график в файл
                plt.savefig(PLOT_FILE)
                plt.close()
                print(f"Гистограмма обновлена и сохранена в {PLOT_FILE}")
            else:
                print("Лог-файл пуст, гистограмма не построена.")
        except Exception as e:
            print(f"Ошибка при построении гистограммы: {e}")
    else:
        print(f"Файл {LOG_FILE} не найден.")
        
        
if __name__ == "__main__":
    print("Сервис plot запущен. Нажмите CTRL+C для завершения.")
    while True:
        plot_histogram()
        time.sleep(REFRESH_INTERVAL)
