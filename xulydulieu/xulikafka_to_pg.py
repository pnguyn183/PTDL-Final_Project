from kafka import KafkaConsumer
import pandas as pd
import json
from sqlalchemy import create_engine
import psycopg2

# Kết nối với Kafka Consumer
consumer = KafkaConsumer(
    'cars',  # Tên topic
    bootstrap_servers='localhost:9092',  # Địa chỉ Kafka server
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Tạo danh sách để lưu dữ liệu
data = []

# Nhận dữ liệu từ topic và lưu vào danh sách
print("Đang nhận dữ liệu từ Kafka...")
for message in consumer:
    data.append(message.value)
    if len(data) >= 1155:  # Giới hạn dữ liệu
        break

# Chuyển đổi dữ liệu thành DataFrame
df = pd.DataFrame(data)

# Step 2: Select required columns
required_columns = ['car_name', 'car_price', 'year', 'style', 'madein', 'kilometer', 'province', 'district', 'gearbox', 'fuel', 'descri']

# Check for missing columns
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print("Missing columns:", missing_columns)
else:
    output_df = df[required_columns]  # Select only the necessary columns

    # Define a function to process price values
    def process_price(price):
        price = str(price).strip()
        if 'tỉ' in price and 'triệu' in price:
            parts = price.split('tỉ')
            ti_part = parts[0].replace("tỉ", "").strip()
            trieu_part = parts[1].replace("triệu", "").strip()
            total_price = int(ti_part) * 1000000000 + int(trieu_part) * 1000000
        elif 'tỉ' in price:
            total_price = int(price.replace("tỉ", "").strip()) * 1000000000
        elif 'triệu' in price:
            total_price = int(price.replace("triệu", "").strip()) * 1000000
        else:
            total_price = 0
        return total_price

    # Step 1: Process the "Price" column using .loc
    output_df.loc[:, 'car_price'] = output_df['car_price'].apply(process_price)

    # Step 2: Process the "Kilometer" column using .loc, handling empty strings
    output_df.loc[:, 'kilometer'] = (
        output_df['kilometer']
        .fillna('0')  # Replace NaN with '0' as string
        .str.replace("km", "")
        .str.replace(".", "")
        .str.replace(" ", "")
        .replace('', '0')  # Replace empty strings with '0'
        .astype(int)  # Convert to integer after cleaning
    )

    # Step 3: Clean up the "Description" column using .loc
    replace_list = [
        ("-", ""), ("  ", " "), ("•", ""), (" .", "."), ("..", "."), 
        ("✅", ""), ("_", ""), (" ️️", ""), ("☎☎", ""), ("\"", ""), ("nt", "nội thất")
    ]
    
    for old_value, new_value in replace_list:
        output_df.loc[:, 'descri'] = output_df['descri'].str.replace(old_value, new_value)

    output_df.loc[:, 'descri'] = output_df['descri'].str.replace(r'\s+', ' ', regex=True).str.strip()

    # Export the processed data to a new CSV file
    output_df.to_csv('cars_data_xuli.csv', index=False, encoding='utf-8')
    print("Dữ liệu sau xử lí đã được xuất ra file cars_data_xuli.csv.")
    print("Tiến hành đổ dữ liệu sang PostgreSQL...")
    # Bước 1: Đọc dữ liệu từ file CSV
    input_csv_file = 'cars_data_xuli.csv'  # Thay đổi tên file CSV của bạn
    df = pd.read_csv(input_csv_file, encoding='utf-8')

    # Bước 2: Chọn các cột cần thiết
    required_columns = ['car_name', 'car_price', 'year', 'style', 'madein', 'kilometer', 'province', 'district', 'gearbox', 'fuel', 'descri']

    # Kiểm tra xem các cột cần thiết có trong DataFrame không
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print("Các cột thiếu:", missing_columns)
        exit()

    # Chỉ giữ lại các cột cần thiết
    output_df = df[required_columns]

    # Bước 3: Kết nối tới PostgreSQL
    engine = create_engine('postgresql://postgres:18032004@localhost:5432/cardb')
    postgres_conn = psycopg2.connect(
        host="localhost",
        database="cardb",
        user="postgres",
        password="18032004",
        options="-c client_encoding=UTF8"
    )
    postgres_cursor = postgres_conn.cursor()

    # Tạo bảng Province
    postgres_cursor.execute("""
        CREATE TABLE IF NOT EXISTS Province (
            province_id SERIAL PRIMARY KEY,
            province_name VARCHAR(100) UNIQUE NOT NULL
        );
    """)

    # Tạo bảng District
    postgres_cursor.execute("""
        CREATE TABLE IF NOT EXISTS District (
            district_id SERIAL PRIMARY KEY,
            district_name VARCHAR(100) NOT NULL,
            province_id INT REFERENCES Province(province_id) ON DELETE CASCADE,
            UNIQUE(district_name, province_id)
        );
    """)

    # Tạo bảng Style
    postgres_cursor.execute("""
        CREATE TABLE IF NOT EXISTS Style (
            style_id SERIAL PRIMARY KEY,
            style_name VARCHAR(100) UNIQUE NOT NULL
        );
    """)

    # Tạo bảng Car
    postgres_cursor.execute("""
        CREATE TABLE IF NOT EXISTS Car (
            car_id SERIAL PRIMARY KEY,
            car_name VARCHAR(255) NOT NULL,
            car_price BIGINT NOT NULL,
            year INT NOT NULL,
            style_id INT REFERENCES Style(style_id) ON DELETE SET NULL,
            madein VARCHAR(100),
            kilometer INT NOT NULL,
            province_id INT REFERENCES Province(province_id) ON DELETE SET NULL,
            district_id INT REFERENCES District(district_id) ON DELETE SET NULL,
            gearbox VARCHAR(50),
            fuel VARCHAR(50),
            descri TEXT
        );
    """)

    # Lưu thay đổi
    postgres_conn.commit()

    # Bước 4: Chèn dữ liệu vào các bảng trong PostgreSQL
    # Chèn dữ liệu vào bảng Province
    for province in output_df['province'].unique():
        postgres_cursor.execute("""
            INSERT INTO Province (province_name) VALUES (%s) ON CONFLICT (province_name) DO NOTHING RETURNING province_id;
        """, (province,))
        postgres_conn.commit()

    print(f"Đã chèn {len(output_df['province'].unique())} tỉnh vào bảng Province.")

    # Bước 1: Loại bỏ các giá trị NaN trong cột 'district' và 'province'
    output_df = output_df.dropna(subset=['district', 'province'])

    # Chèn dữ liệu vào bảng District
    for district in output_df['district'].unique():
        # Lọc ra tỉnh tương ứng với district
        province_rows = output_df.loc[output_df['district'] == district, 'province']
        
        # Kiểm tra nếu có tỉnh cho district này
        if province_rows.empty:
            print(f"Không tìm thấy tỉnh cho quận {district}. Bỏ qua quận này.")
            continue  # Bỏ qua district này nếu không tìm thấy tỉnh tương ứng
        
        province_name = province_rows.values[0]
        
        # Tìm id của tỉnh
        postgres_cursor.execute("""
            SELECT province_id FROM Province WHERE province_name = %s;
        """, (province_name,))
        result = postgres_cursor.fetchone()
        
        if result is None:
            print(f"Không tìm thấy province_id cho tỉnh {province_name}. Bỏ qua quận {district}.")
            continue
        
        province_id = result[0]
                
        # Chèn district vào bảng District
        postgres_cursor.execute("""
            INSERT INTO District (district_name, province_id) VALUES (%s, %s) 
            ON CONFLICT (district_name, province_id) DO NOTHING RETURNING district_id;
        """, (district, province_id))
        postgres_conn.commit()

    print(f"Đã chèn {len(output_df['district'].unique())} quận vào bảng District.")

    # Chèn dữ liệu vào bảng Style
    style_names = output_df['style'].unique()
    for style in style_names:
        postgres_cursor.execute("""
            INSERT INTO Style (style_name) VALUES (%s) ON CONFLICT (style_name) DO NOTHING RETURNING style_id;
        """, (style,))
        postgres_conn.commit()

    print(f"Đã chèn {len(style_names)} kiểu dáng vào bảng Style.")

    # Chèn dữ liệu vào bảng Car
    for index, row in output_df.iterrows():
        # Lấy id của style, province và district tương ứng
        postgres_cursor.execute("""
            SELECT style_id FROM Style WHERE style_name = %s;
        """, (row['style'],))
        style_id = postgres_cursor.fetchone()[0]

        postgres_cursor.execute("""
            SELECT province_id FROM Province WHERE province_name = %s;
        """, (row['province'],))
        province_id = postgres_cursor.fetchone()[0]

        postgres_cursor.execute("""
            SELECT district_id FROM District WHERE district_name = %s AND province_id = %s;
        """, (row['district'], province_id))
        district_id = postgres_cursor.fetchone()[0]

        # Chèn dữ liệu vào bảng Car
        postgres_cursor.execute("""
            INSERT INTO Car (car_name, car_price, year, style_id, madein, kilometer, province_id, district_id, gearbox, fuel, descri) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (row['car_name'], row['car_price'], row['year'], style_id, row['madein'], row['kilometer'], province_id, district_id, row['gearbox'], row['fuel'], row['descri']))
        postgres_conn.commit()

    print("Dữ liệu đã được lưu vào cơ sở dữ liệu.")

    # Đóng kết nối
    postgres_cursor.close()
    postgres_conn.close()