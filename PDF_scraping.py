import re
import pdfplumber
import pandas as pd


def extract_tables_from_pdf(pdf_location, starting_page, ending_page):
    extracted_tables = []

    with pdfplumber.open(pdf_location) as pdf:
        for i in range(starting_page - 1, ending_page):
            try:
                page = pdf.pages[i]
                tables = page.extract_tables()
                for table in tables:
                    # avoid to make first row of table as a header
                    data = table[0:]
                    # Create a DataFrame with default column names, fill the nan values and append the data
                    df1 = pd.DataFrame(data)
                    df1.ffill(axis=1, inplace=True)
                    extracted_tables.append(df1)
                    # print(f"Extracted table from page {i + 1}:\n{df}\n")
            except Exception as e:
                print(f"An error occurred while extracting tables from page {i + 1}: {e}")

    return extracted_tables


def extract_order_no_rows(extract_tables):
    order_rows_transposed = []

    for table in extract_tables:
        for row in table.iterrows():
            if any(cell == 'Order No.' for cell in row[1]):
                # Remove the first column and transpose the row
                transposed_row = row[1][1:].transpose()
                order_rows_transposed.append(transposed_row)

    if order_rows_transposed:
        order_df = pd.concat(order_rows_transposed, ignore_index=True)
        return order_df
    else:
        return None


def extract_weight_rows(extract_tables):
    # finding the rows having Weight, in it and then scraping that row and then taking transpose of it
    weight_rows_transposed = []

    for table in extract_tables:
        for row in table.iterrows():
            if any(cell == 'Weight' for cell in row[1]):
                numeric_values = [re.search(r'\d+', str(cell)).group() for cell in row[1][1:]]
                transposed_row = pd.DataFrame([numeric_values]).transpose()
                weight_rows_transposed.append(transposed_row)

    if weight_rows_transposed:
        weigh_df = pd.concat(weight_rows_transposed, ignore_index=True)
        return weigh_df
    else:
        return None


def joining_Dataframes(order_no, weight):
    joined_dataframe = pd.concat([order_no, weight], axis=1, join='inner')
    return joined_dataframe


if __name__ == "__main__":
    pdf_path = "/home/bilalahmad/PycharmProjects/web_scraping/0.pdf"
    start_page = 52
    end_page = 62
    extract_tables = extract_tables_from_pdf(pdf_path, start_page, end_page)
    order_no_df = extract_order_no_rows(extract_tables)
    weight_df = extract_weight_rows(extract_tables)
    df = joining_Dataframes(order_no_df, weight_df)
    df.to_csv('/home/bilalahmad/Desktop/Kathrein_2_DATA1.csv', index=False)
