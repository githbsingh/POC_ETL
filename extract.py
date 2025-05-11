from faker import Faker
import csv
import random
from google.cloud import storage
import os

# Initialize Faker
fake = Faker()
Faker.seed(42)

departments = ['Engineering', 'Sales', 'HR', 'Marketing', 'Finance', 'Operations']

# Generate fake employee data
def generate_employee_data(num_employees=10):
    employee_data = []
    for _ in range(num_employees):
        salary = round(random.uniform(300000, 2500000), 2)
        employee = {
            "employee_id": fake.unique.random_int(min=1000, max=9999),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "ssn": fake.ssn(),
            "date_of_birth": fake.date_of_birth(minimum_age=22, maximum_age=65).isoformat(),
            "address": fake.address().replace('\n', ', '),
            "department": random.choice(departments),
            "salary": salary,
            "password": fake.password(length=12, special_chars=True, digits=True, upper_case=True, lower_case=True),
        }
        employee_data.append(employee)
    return employee_data

# Save data to CSV file
def save_to_csv(data, filename='fake_employees.csv'):
    fieldnames = data[0].keys()
    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"CSV saved locally as {filename}")

# Upload CSV to GCS bucket
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")

# Main
if __name__ == "__main__":
    file_name = "fake_employees.csv"
    bucket_name = "blt-employee-data"  # 🔁 Replace this with your bucket name
    destination_blob = "data/" + file_name  # Destination path in the bucket

    data = generate_employee_data(num_employees=100)
    save_to_csv(data, file_name)
    upload_to_gcs(bucket_name, file_name, destination_blob)
