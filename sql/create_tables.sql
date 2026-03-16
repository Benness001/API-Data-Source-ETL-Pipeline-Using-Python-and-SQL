CREATE TABLE dbo.Users (
-- Original API Fields
id INT PRIMARY KEY,
name NVARCHAR(255) NOT NULL,
username NVARCHAR(100),
email NVARCHAR(255),
address_city NVARCHAR(100),
company_name NVARCHAR(255),

-- Engineered Features (Calculated in Python)
email_domain NVARCHAR(100),
name_length INT,
username_length INT,
location_company NVARCHAR(500),

-- Metadata for Auditing
extracted_at DATETIME DEFAULT GETDATE()
);

SELECT * FROM Users

DROP TABLE Users