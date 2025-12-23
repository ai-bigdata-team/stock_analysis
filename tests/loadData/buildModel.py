import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix
from sklearn.feature_selection import SelectKBest, f_classif, mutual_info_classif
import warnings

warnings.filterwarnings('ignore')

# ============================================
# PATHS
# ============================================
MONTHLY_DIR = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_feature_batch_monthly"
WEEKLY_DIR = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\loadData\stocks_ohlcv_feature_batch_weekly"
OUTPUT_DIR = r"F:\TinHoc\BinningMini\BigData_BTL\stock_analysis\tests\models"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Feature selection config
TOP_K_FEATURES = 50


# ============================================
# DATA LOADING FUNCTION
# ============================================
def load_all_data(base_dir, freq_name):
    """Load all parquet files from all stock folders"""
    all_data = []

    stock_folders = [f for f in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, f))]
    print(f"📂 Found {len(stock_folders)} stock folders in {freq_name}")

    for stock_folder in stock_folders:
        stock_path = os.path.join(base_dir, stock_folder)
        parquet_files = [f for f in os.listdir(stock_path) if f.endswith('.parquet')]

        for file in parquet_files:
            file_path = os.path.join(stock_path, file)
            df = pd.read_parquet(file_path)

            # Extract date from filename
            date_str = file.replace('.parquet', '')
            df['date'] = pd.to_datetime(date_str)

            all_data.append(df)
            print(stock_path, date_str)

    if len(all_data) == 0:
        return None

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values('date').reset_index(drop=True)

    print(f"  ✅ Loaded {len(combined_df)} samples")
    print(f"  📅 Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")

    return combined_df


# ============================================
# SPLIT DATA BY DATE
# ============================================
def split_data_by_date(df, train_end, valid_end):
    """Split data into train/valid/test by date"""

    train_df = df[df['date'] < train_end].copy()
    valid_df = df[(df['date'] >= train_end) & (df['date'] < valid_end)].copy()
    test_df = df[df['date'] >= valid_end].copy()

    print(f"\n📊 Data Split:")
    print(f"  Train: {len(train_df)} samples ({train_df['date'].min()} to {train_df['date'].max()})")
    print(f"  Valid: {len(valid_df)} samples ({valid_df['date'].min()} to {valid_df['date'].max()})")
    print(f"  Test:  {len(test_df)} samples ({test_df['date'].min()} to {test_df['date'].max()})")

    return train_df, valid_df, test_df


# ============================================
# PREPARE FEATURES
# ============================================
def prepare_features(train_df, valid_df, test_df):
    """Prepare features and labels, handle missing values"""

    # Separate features and labels
    exclude_cols = ['stock_code', 'date', 'label']
    feature_cols = [col for col in train_df.columns if col not in exclude_cols]

    X_train = train_df[feature_cols].copy()
    y_train = train_df['label'].copy()

    X_valid = valid_df[feature_cols].copy()
    y_valid = valid_df['label'].copy()

    X_test = test_df[feature_cols].copy()
    y_test = test_df['label'].copy()

    print(f"\n🔧 Feature Engineering:")
    print(f"  Total features: {len(feature_cols)}")

    # Handle missing values
    print(f"  Handling missing values...")
    X_train = X_train.fillna(0)
    X_valid = X_valid.fillna(0)
    X_test = X_test.fillna(0)

    # Handle infinite values
    X_train = X_train.replace([np.inf, -np.inf], 0)
    X_valid = X_valid.replace([np.inf, -np.inf], 0)
    X_test = X_test.replace([np.inf, -np.inf], 0)

    # Standardize features
    print(f"  Standardizing features...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_valid_scaled = scaler.transform(X_valid)
    X_test_scaled = scaler.transform(X_test)

    # Convert back to DataFrame
    X_train_scaled = pd.DataFrame(X_train_scaled, columns=feature_cols)
    X_valid_scaled = pd.DataFrame(X_valid_scaled, columns=feature_cols)
    X_test_scaled = pd.DataFrame(X_test_scaled, columns=feature_cols)

    print(f"\n📈 Label Distribution:")
    print(f"  Train - Up: {y_train.sum()}/{len(y_train)} ({y_train.mean() * 100:.1f}%)")
    print(f"  Valid - Up: {y_valid.sum()}/{len(y_valid)} ({y_valid.mean() * 100:.1f}%)")
    print(f"  Test  - Up: {y_test.sum()}/{len(y_test)} ({y_test.mean() * 100:.1f}%)")

    return X_train_scaled, X_valid_scaled, X_test_scaled, y_train, y_valid, y_test, feature_cols, scaler


# ============================================
# FEATURE SELECTION
# ============================================
def select_best_features(X_train, X_valid, y_train, y_valid, feature_cols, k=TOP_K_FEATURES):
    """Select top K features using multiple methods and ensemble"""

    print(f"\n{'=' * 60}")
    print(f"🔍 Feature Selection - Selecting Top {k} Features")
    print(f"{'=' * 60}")

    # Combine train and valid for feature selection
    X_combined = pd.concat([X_train, X_valid], axis=0)
    y_combined = pd.concat([y_train, y_valid], axis=0)

    # Method 1: F-statistic (ANOVA)
    print("  📊 Method 1: F-statistic (ANOVA)...")
    selector_f = SelectKBest(f_classif, k=k)
    selector_f.fit(X_combined, y_combined)
    scores_f = pd.DataFrame({
        'feature': feature_cols,
        'f_score': selector_f.scores_
    }).sort_values('f_score', ascending=False)

    # Method 2: Mutual Information
    print("  📊 Method 2: Mutual Information...")
    selector_mi = SelectKBest(mutual_info_classif, k=k)
    selector_mi.fit(X_combined, y_combined)
    scores_mi = pd.DataFrame({
        'feature': feature_cols,
        'mi_score': selector_mi.scores_
    }).sort_values('mi_score', ascending=False)

    # Method 3: Random Forest Feature Importance
    print("  📊 Method 3: Random Forest Importance...")
    rf_temp = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    rf_temp.fit(X_combined, y_combined)
    scores_rf = pd.DataFrame({
        'feature': feature_cols,
        'rf_importance': rf_temp.feature_importances_
    }).sort_values('rf_importance', ascending=False)

    # Ensemble: Rank features by each method and take top K by average rank
    print("  🎯 Ensemble ranking...")
    scores_f['rank_f'] = scores_f.index + 1
    scores_mi['rank_mi'] = scores_mi.index + 1
    scores_rf['rank_rf'] = scores_rf.index + 1

    # Merge all scores
    merged = scores_f[['feature', 'f_score', 'rank_f']].merge(
        scores_mi[['feature', 'mi_score', 'rank_mi']], on='feature'
    ).merge(
        scores_rf[['feature', 'rf_importance', 'rank_rf']], on='feature'
    )

    # Calculate average rank
    merged['avg_rank'] = (merged['rank_f'] + merged['rank_mi'] + merged['rank_rf']) / 3
    merged = merged.sort_values('avg_rank')

    # Select top K features
    selected_features = merged.head(k)['feature'].tolist()

    print(f"\n  ✅ Selected {len(selected_features)} features")
    print(f"\n  📋 Top 10 Features:")
    for idx, row in merged.head(10).iterrows():
        print(f"    {row['feature'][:50]:<50} (Avg Rank: {row['avg_rank']:.1f})")

    # Filter datasets
    X_train_selected = X_train[selected_features]
    X_valid_selected = X_valid[selected_features]

    return X_train_selected, X_valid_selected, selected_features, merged


# ============================================
# HYPERPARAMETER TUNING
# ============================================
def tune_hyperparameters(X_train, X_valid, y_train, y_valid):
    """Tune hyperparameters using train+valid data"""

    print(f"\n{'=' * 60}")
    print(f"⚙️  Hyperparameter Tuning")
    print(f"{'=' * 60}")

    # Combine train and valid for tuning
    X_combined = pd.concat([X_train, X_valid], axis=0)
    y_combined = pd.concat([y_train, y_valid], axis=0)

    best_models = {}

    # 1. Logistic Regression
    print("\n  🔧 Tuning Logistic Regression...")
    lr_params = {
        'C': [0.01, 0.1, 1, 10, 100],
        'penalty': ['l1', 'l2'],
        'solver': ['liblinear']
    }
    lr_grid = GridSearchCV(
        LogisticRegression(max_iter=1000, random_state=42),
        lr_params,
        cv=5,
        scoring='roc_auc',
        n_jobs=-1
    )
    lr_grid.fit(X_combined, y_combined)
    best_models['Logistic Regression'] = lr_grid.best_estimator_
    print(f"    Best params: {lr_grid.best_params_}")
    print(f"    Best CV AUC: {lr_grid.best_score_:.4f}")

    # 2. Random Forest
    print("\n  🔧 Tuning Random Forest...")
    rf_params = {
        'n_estimators': [50, 100, 200],
        'max_depth': [5, 10, 15, 20],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4]
    }
    rf_grid = GridSearchCV(
        RandomForestClassifier(random_state=42, n_jobs=-1),
        rf_params,
        cv=5,
        scoring='roc_auc',
        n_jobs=-1
    )
    rf_grid.fit(X_combined, y_combined)
    best_models['Random Forest'] = rf_grid.best_estimator_
    print(f"    Best params: {rf_grid.best_params_}")
    print(f"    Best CV AUC: {rf_grid.best_score_:.4f}")

    # 3. Gradient Boosting
    print("\n  🔧 Tuning Gradient Boosting...")
    gb_params = {
        'n_estimators': [50, 100, 200],
        'max_depth': [3, 5, 7],
        'learning_rate': [0.01, 0.05, 0.1],
        'subsample': [0.8, 1.0]
    }
    gb_grid = GridSearchCV(
        GradientBoostingClassifier(random_state=42),
        gb_params,
        cv=5,
        scoring='roc_auc',
        n_jobs=-1
    )
    gb_grid.fit(X_combined, y_combined)
    best_models['Gradient Boosting'] = gb_grid.best_estimator_
    print(f"    Best params: {gb_grid.best_params_}")
    print(f"    Best CV AUC: {gb_grid.best_score_:.4f}")

    return best_models


# ============================================
# TRAIN AND EVALUATE MODEL
# ============================================
def train_and_evaluate_final(X_train, X_valid, X_test, y_train, y_valid, y_test, best_models):
    """Train final models on train+valid and evaluate on test"""

    print(f"\n{'=' * 60}")
    print(f"🚀 Final Training on Train + Valid")
    print(f"{'=' * 60}")

    # Combine train and valid for final training
    X_train_val = pd.concat([X_train, X_valid], axis=0)
    y_train_val = pd.concat([y_train, y_valid], axis=0)

    results = []

    for model_name, model in best_models.items():
        print(f"\n  🤖 Training {model_name}...")

        # Train on combined data
        model.fit(X_train_val, y_train_val)

        # Predict on all sets
        y_train_pred_proba = model.predict_proba(X_train)[:, 1]
        y_valid_pred_proba = model.predict_proba(X_valid)[:, 1]
        y_test_pred_proba = model.predict_proba(X_test)[:, 1]

        y_train_pred = model.predict(X_train)
        y_valid_pred = model.predict(X_valid)
        y_test_pred = model.predict(X_test)

        # Calculate AUC
        train_auc = roc_auc_score(y_train, y_train_pred_proba)
        valid_auc = roc_auc_score(y_valid, y_valid_pred_proba)
        test_auc = roc_auc_score(y_test, y_test_pred_proba)

        print(f"    Train AUC: {train_auc:.4f}")
        print(f"    Valid AUC: {valid_auc:.4f}")
        print(f"    Test  AUC: {test_auc:.4f}")

        # Detailed metrics for test set
        print(f"\n    📋 Test Set Classification Report:")
        print(classification_report(y_test, y_test_pred, target_names=['Down (0)', 'Up (1)'], zero_division=0))

        print(f"\n    🔢 Test Set Confusion Matrix:")
        cm = confusion_matrix(y_test, y_test_pred)
        print(f"                    Predicted")
        print(f"                  Down    Up")
        print(f"    Actual Down   {cm[0, 0]:4d}  {cm[0, 1]:4d}")
        print(f"           Up     {cm[1, 0]:4d}  {cm[1, 1]:4d}")

        results.append({
            'model_name': model_name,
            'train_auc': train_auc,
            'valid_auc': valid_auc,
            'test_auc': test_auc,
            'model': model
        })

    return results


# ============================================
# MAIN PROCESSING
# ============================================
def process_frequency(base_dir, freq_name, train_end, valid_end):
    """Process one frequency (weekly or monthly)"""

    print(f"\n{'=' * 80}")
    print(f"🚀 Processing {freq_name.upper()} Data")
    print(f"{'=' * 80}")

    # Load data
    df = load_all_data(base_dir, freq_name)
    if df is None:
        print(f"⚠️  No data found for {freq_name}")
        return None

    # Split data
    train_df, valid_df, test_df = split_data_by_date(df, train_end, valid_end)

    # Prepare features
    X_train, X_valid, X_test, y_train, y_valid, y_test, feature_cols, scaler = prepare_features(
        train_df, valid_df, test_df
    )

    # STEP 1: Feature Selection using Train + Valid
    X_train_selected, X_valid_selected, selected_features, feature_scores = select_best_features(
        X_train, X_valid, y_train, y_valid, feature_cols, k=TOP_K_FEATURES
    )
    X_test_selected = X_test[selected_features]

    # STEP 2: Hyperparameter Tuning using Train + Valid
    best_models = tune_hyperparameters(
        X_train_selected, X_valid_selected, y_train, y_valid
    )

    # STEP 3: Final Training on Train + Valid, Evaluate on Test
    results = train_and_evaluate_final(
        X_train_selected, X_valid_selected, X_test_selected,
        y_train, y_valid, y_test,
        best_models
    )

    # Summary
    print(f"\n{'=' * 80}")
    print(f"📊 {freq_name.upper()} Summary - AUC Scores")
    print(f"{'=' * 80}")
    print(f"{'Model':<25} {'Train AUC':>12} {'Valid AUC':>12} {'Test AUC':>12}")
    print(f"{'-' * 80}")
    for result in results:
        print(
            f"{result['model_name']:<25} {result['train_auc']:>12.4f} {result['valid_auc']:>12.4f} {result['test_auc']:>12.4f}")

    return results, selected_features, feature_scores


# ============================================
# RUN ANALYSIS
# ============================================
if __name__ == "__main__":

    # Define date splits
    train_end = datetime(2025, 7, 2)  # Train: 2025-01-01 to 2025-07-01
    valid_end = datetime(2025, 9, 2)  # Valid: 2025-07-02 to 2025-09-01
    # Test:  2025-09-02 to 2025-11-01

    print("=" * 80)
    print("📈 STOCK PREDICTION MODEL TRAINING")
    print("=" * 80)
    print(f"Train Period: Start to {train_end.strftime('%Y-%m-%d')}")
    print(f"Valid Period: {train_end.strftime('%Y-%m-%d')} to {valid_end.strftime('%Y-%m-%d')}")
    print(f"Test Period:  {valid_end.strftime('%Y-%m-%d')} to End")
    print(f"\n🎯 Pipeline:")
    print(f"  1. Feature Selection: Select top {TOP_K_FEATURES} features using Train + Valid")
    print(f"  2. Hyperparameter Tuning: Optimize models using Train + Valid with 5-fold CV")
    print(f"  3. Final Training: Train on Train + Valid, evaluate on Test")

    # Process Weekly
    weekly_results, weekly_features, weekly_scores = process_frequency(
        WEEKLY_DIR, "weekly", train_end, valid_end
    )

    # Process Monthly
    monthly_results, monthly_features, monthly_scores = process_frequency(
        MONTHLY_DIR, "monthly", train_end, valid_end
    )

    # Final Summary
    print("\n" + "=" * 80)
    print("🎉 FINAL SUMMARY")
    print("=" * 80)

    if weekly_results:
        print("\n📅 WEEKLY Models - Test AUC:")
        for result in weekly_results:
            print(f"  {result['model_name']:<25} {result['test_auc']:.4f}")

    if monthly_results:
        print("\n📅 MONTHLY Models - Test AUC:")
        for result in monthly_results:
            print(f"  {result['model_name']:<25} {result['test_auc']:.4f}")

    print("\n✅ Training completed!")