import mlflow
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from xgboost import XGBClassifier, plot_importance
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, classification_report
from sqlalchemy import create_engine
from sklearn.model_selection import GridSearchCV

def train_crypto_model(db_creds, mlflow_uri):
    # 1. Загрузка данных
    engine = create_engine(db_creds)
    df = pd.read_sql("SELECT * FROM dev_coingecko_marts.ml_features_price_changes where target_direction is not null and lag_price_7d is not null", engine)
    
    # Сортируем по дате для правильного разделения
    df = df.sort_values('report_date')
    df['coin_id'] = df['coin_id'].astype('category')
    
    # 2. Подготовка признаков (используем скользящие средние)
    features = ['coin_id', 
                'price', 'lag_price_1d', 'lag_price_7d', 'price_moving_avg_7d', 
	            'cap', 'lag_cap_1d', 'lag_cap_7d', 'cap_moving_avg_7d', 
	            'volume', 'lag_volume_1d', 'lag_volume_7d', 'volume_moving_avg_7d',
	            'price_diff_1d', 'price_diff_7d', 'price_diff_avg_7d',
	            'cap_diff_1d', 'cap_diff_7d', 'cap_diff_avg_7d',
	            'volume_diff_1d', 'volume_diff_7d', 'volume_diff_avg_7d', 
                'rsi_7d']
    X = df[features]
    y = df['target_direction']
    
    # Разделение (последние 20% данных — для теста)
    split = int(len(df) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    # 3. MLflow Tracking
    mlflow.set_tracking_uri(mlflow_uri)
    with mlflow.start_run(run_name="price_changes_direction_xgboost"):
        # 1. Задаем сетку параметров
        param_grid = {
            'n_estimators': [50, 100, 200],
            'max_depth': [3, 5, 7],
            'learning_rate': [0.05, 0.1, 0.2]
        }

        # 2. Запускаем поиск (cv=5 означает кросс-валидацию по 5 частям данных)
        grid_search = GridSearchCV(
            estimator=XGBClassifier(enable_categorical=True),
            param_grid=param_grid,
            cv=5,
            scoring='accuracy'
        )

        # 3. ОБУЧЕНИЕ
        grid_search.fit(X_train, y_train)
        # Извлекаем лучшую модель
        best_model = grid_search.best_estimator_        
        
        # 4. Метрики
        preds = best_model.predict(X_test)
        metrics = {
            "accuracy": accuracy_score(y_test, preds),
            "precision": precision_score(y_test, preds),
            "recall": recall_score(y_test, preds),
            "f1_score": f1_score(y_test, preds)
        }

        # Получаем вероятности для каждого класса [вероятность_0, вероятность_1]
        # probs = grid_search.predict_proba(X_test)
        # Вероятность роста (класса 1)
        # growth_probability = probs[:, 1]

        # 5. Feature Importance (График)
        plt.figure(figsize=(14, 8))
        plot_importance(best_model)
        plt.title("Feature Importance - Price Direction")
        plt.tight_layout()
        plt.savefig("feature_importance_price_direction.png")
        mlflow.log_artifact("feature_importance_price_direction.png") # Отправляем в MLflow
        
        # 6. Confusion Matrix (Тепловая карта)
        plt.figure(figsize=(8, 6))
        cm = confusion_matrix(y_test, preds)
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
        plt.xlabel('Predicted')
        plt.ylabel('Actual')
        plt.title('Confusion Matrix - Price Direction')
        plt.tight_layout()
        plt.savefig("confusion_matrix_price_direction.png")
        mlflow.log_artifact("confusion_matrix_price_direction.png")
        
        # 7. Логируем параметры и метрики
        best_params = grid_search.best_params_
        mlflow.log_params(best_params)
        mlflow.log_metrics(metrics)
        # логирование фичей
        features_list = X.columns.tolist()
        mlflow.log_param("features", ", ".join(features_list))
        mlflow.log_param("num_features", len(features_list))
        # логирование модели
        mlflow.sklearn.log_model(best_model, "price_changes_direction_xgboost_model")
        
        print(f"Model trained. Accuracy: {metrics['accuracy']}")