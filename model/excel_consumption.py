from typing import List

import pandas as pd
from sqlalchemy.orm import Session

import config
from config import production
from model.ProductionList import ProductionList
from model.Shift import Shift
from model.consumptions.consumption import Consumption
from model.consumptions.material import Material
from model.gypsum_board.BoardProduction import BoardProduction
from model.gypsum_board.GypsumBoard import GypsumBoard


def read_consumption():
    data = pd.read_excel(config.consumptions)
    print(data[:5])
    return data

def select_by_date_from_excel(board_production, excel_consumption: pd.DataFrame, session: Session):
    data_to_import: List[Consumption] = []
    production_date = board_production.production_log.production_date
    product: GypsumBoard= board_production.gypsum_board
    shift: Shift = board_production.production_log.shift
    # print(excel_consumption.dtypes)
    # print(production_date, shift, product)
    # print(production_date, "->", excel_consumption['date'].iloc[0])
    # print(f"Production Date: {production_date}", production_date in excel_consumption["date"].unique())
    # print(f"Shift Name: {shift.name}", shift.name in excel_consumption["shift"].unique())
    # print(f"Trade Mark Name: {product.trade_mark.name}", product.trade_mark.name in excel_consumption["trademark"].unique())
    # print(f"Board Type Name: {product.board_types.name}", product.board_types.name in excel_consumption["type"].unique())
    # print(f"Edge Name: {product.edge.name}", product.edge.name in excel_consumption["edge"].unique() )
    # print(f"Length Value: {product.length.value}", product.length.value in excel_consumption["length"].unique())
    # print(f"Width Value: {product.width.value}", product.width.value in excel_consumption["width"].unique())
    # print(f"Thickness Value: {product.thickness.value}", product.thickness.value in excel_consumption["thickness"].unique())

    filtered_df = excel_consumption[
        (excel_consumption["date"] == production_date) &
        (excel_consumption["shift"] == shift.name) &
        (excel_consumption["trademark"] == product.trade_mark.name) &
        (excel_consumption["type"] == product.board_types.name) &
        (excel_consumption["edge"] == product.edge.name) &
        (excel_consumption["length"] == product.length.value) &
        (excel_consumption["width"] == product.width.value) &
        (excel_consumption["thickness"] == product.thickness.value)
        ]
    if filtered_df.empty:
        # print('Какая то шляпа')
        return
    else:
        for _, row in filtered_df.iterrows():
            # print(session.query(Material).all())
            material: Material | None = session.query(Material).filter_by(name=row['material']).first()
            # print("Найден материал", material)
            quantity = row.get('quantity', 0)
            if material:
                consumption = Consumption(production_list=board_production.production_log, material=material, quantity=quantity)
                # print(consumption)
                data_to_import.append(consumption)
            else:
                print(f"Material with name '{row['material']}' not found")
        return data_to_import