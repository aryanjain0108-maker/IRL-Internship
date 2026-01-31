import pandas as pd
import json
import re
import os

# Set the working directory
base_path = r"c:\Users\aryan\OneDrive\Desktop\New folder (2)"

print("=" * 80)
print("STEP 1: LOADING CSV DATA (Orders)")
print("=" * 80)

# Load orders CSV
orders_df = pd.read_csv(os.path.join(base_path, 'orders.csv'))
print(f"✓ Loaded orders.csv: {orders_df.shape[0]} rows × {orders_df.shape[1]} columns")
print(f"  Columns: {list(orders_df.columns)}")
print(f"  Sample data:\n{orders_df.head()}\n")

print("=" * 80)
print("STEP 2: LOADING JSON DATA (Users)")
print("=" * 80)

# Load users JSON
with open(os.path.join(base_path, 'users.json'), 'r') as f:
    users_data = json.load(f)

users_df = pd.DataFrame(users_data)
print(f"✓ Loaded users.json: {users_df.shape[0]} rows × {users_df.shape[1]} columns")
print(f"  Columns: {list(users_df.columns)}")
print(f"  Sample data:\n{users_df.head()}\n")

print("=" * 80)
print("STEP 3: LOADING SQL DATA (Restaurants)")
print("=" * 80)

# Load restaurants SQL
with open(os.path.join(base_path, 'restaurants.sql'), 'r') as f:
    sql_content = f.read()

# Parse SQL INSERT statements
restaurants_list = []
insert_pattern = r"INSERT INTO restaurants VALUES \((\d+),\s*'([^']+)',\s*'([^']+)',\s*([\d.]+)\)"
matches = re.findall(insert_pattern, sql_content)

for match in matches:
    restaurants_list.append({
        'restaurant_id': int(match[0]),
        'restaurant_name': match[1],
        'cuisine': match[2],
        'rating': float(match[3])
    })

restaurants_df = pd.DataFrame(restaurants_list)
print(f"✓ Loaded restaurants.sql: {restaurants_df.shape[0]} rows × {restaurants_df.shape[1]} columns")
print(f"  Columns: {list(restaurants_df.columns)}")
print(f"  Sample data:\n{restaurants_df.head()}\n")

print("=" * 80)
print("STEP 4: MERGING DATASETS")
print("=" * 80)

# Merge orders with users (left join on user_id)
print("\n1. Joining orders with users on user_id...")
merged_df = orders_df.merge(
    users_df[['user_id', 'name', 'city', 'membership']],
    on='user_id',
    how='left'
)
print(f"   ✓ After joining users: {merged_df.shape[0]} rows × {merged_df.shape[1]} columns")

# Merge with restaurants (left join on restaurant_id)
print("2. Joining with restaurants on restaurant_id...")
final_df = merged_df.merge(
    restaurants_df[['restaurant_id', 'cuisine', 'rating']],
    on='restaurant_id',
    how='left'
)
print(f"   ✓ After joining restaurants: {final_df.shape[0]} rows × {final_df.shape[1]} columns")

print("\n" + "=" * 80)
print("STEP 5: FINAL DATASET OVERVIEW")
print("=" * 80)

print(f"\nFinal Dataset Dimensions: {final_df.shape[0]} rows × {final_df.shape[1]} columns")
print(f"\nFinal Columns:")
for i, col in enumerate(final_df.columns, 1):
    print(f"  {i:2d}. {col}")

print(f"\nData Types:\n{final_df.dtypes}\n")

print(f"Sample of Final Dataset (First 5 rows):")
print(final_df.head().to_string())

print(f"\n\nData Quality Check:")
print(f"  - Total rows: {final_df.shape[0]}")
print(f"  - Missing values by column:")
for col in final_df.columns:
    missing = final_df[col].isna().sum()
    if missing > 0:
        print(f"    • {col}: {missing} ({missing/len(final_df)*100:.2f}%)")

print("\n" + "=" * 80)
print("STEP 6: SAVING FINAL DATASET")
print("=" * 80)

# Save the final dataset
output_path = os.path.join(base_path, 'final_food_delivery_dataset.csv')
final_df.to_csv(output_path, index=False)
print(f"\n✓ Final dataset saved to: {output_path}")
print(f"  File size: {os.path.getsize(output_path) / (1024*1024):.2f} MB")

print("\n" + "=" * 80)
print("DATASET STATISTICS")
print("=" * 80)

print(f"\nOrder Information:")
print(f"  - Date range: {final_df['order_date'].min()} to {final_df['order_date'].max()}")
print(f"  - Total order value: ₹{final_df['total_amount'].sum():,.2f}")
print(f"  - Average order value: ₹{final_df['total_amount'].mean():,.2f}")
print(f"  - Unique users: {final_df['user_id'].nunique()}")
print(f"  - Unique restaurants: {final_df['restaurant_id'].nunique()}")

print(f"\nUser Membership Distribution:")
print(final_df['membership'].value_counts())

print(f"\nCity Distribution:")
print(final_df['city'].value_counts())

print(f"\nCuisine Distribution:")
print(final_df['cuisine'].value_counts())

print(f"\nRestaurant Rating Statistics:")
print(final_df['rating'].describe())

print("\n" + "=" * 80)
print("✓ DATA MERGING COMPLETE!")
print("=" * 80)
