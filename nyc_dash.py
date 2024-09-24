from bokeh.plotting import figure, curdoc
from bokeh.models import Dropdown
from bokeh.layouts import column
import pandas as pd
import dask.dataframe as dd
import os.path

monthly_df = dd.read_csv(os.path.join(os.path.dirname(__file__),"data/monthly_avg.csv"))
monthly_by_zip_df = dd.read_csv(os.path.join(os.path.dirname(__file__),"data/monthly_avg_by_zip.csv"))

monthly_avg_df = monthly_df.compute()
monthly_avg_by_zip_df = monthly_by_zip_df.compute()

zipcodes_menu = []

for zipcode in monthly_avg_by_zip_df['Zipcode'].unique():
    zipcodes_menu.append((str(zipcode), str(zipcode)))


X = monthly_avg_df['Month'].tolist()

f = figure(title="Monthly average create-to-closed time for the year 2020", x_axis_label="Month", y_axis_label="Average create-to-close time (h)")

def zipcode1_selection_callback(event):
    new_data = dict()
    new_data["x"] = X
    new_data["y"] = monthly_avg_by_zip_df.loc[monthly_avg_by_zip_df['Zipcode'] == int(event.item), 'Create-to-closed Time'].tolist()

    ds2.data = new_data

def zipcode2_selection_callback(event):
    new_data = dict()
    new_data["x"] = X
    new_data["y"] = monthly_avg_by_zip_df.loc[monthly_avg_by_zip_df['Zipcode'] == int(event.item), 'Create-to-closed Time'].tolist()

    ds3.data = new_data

renderer = f.line(x=X, y=monthly_avg_df['Create-to-closed Time'].tolist(), legend_label="For all year 2020")
renderer2 = f.line(x=X, y=monthly_avg_by_zip_df.loc[monthly_avg_by_zip_df['Zipcode'] == 10000, 'Create-to-closed Time'].tolist(), legend_label="For zipcode 1", line_color="green")
renderer3 = f.line(x=X, y=monthly_avg_by_zip_df.loc[monthly_avg_by_zip_df['Zipcode'] == 10001, 'Create-to-closed Time'].tolist(), legend_label="For zipcode 2", line_color="red")

zipcode_selector_1 = Dropdown(label="Select a zipcode 1", menu=zipcodes_menu)
zipcode_selector_2 = Dropdown(label="Select a zipcode 2", menu=zipcodes_menu)

zipcode_selector_1.on_event("menu_item_click", zipcode1_selection_callback)
zipcode_selector_2.on_event("menu_item_click", zipcode2_selection_callback)

ds = renderer.data_source
ds2 = renderer2.data_source
ds3 = renderer3.data_source


curdoc().add_root(column(zipcode_selector_1, zipcode_selector_2, f))