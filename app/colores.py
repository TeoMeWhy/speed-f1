# %%

## Código mágico do @dunossauro (pergunte a ele)
def saturar(valor, minimo=0, maximo=255):
    return max(minimo, min(maximo, valor))

def hex_para_rgb(hex_str):
    hex_str = hex_str.lstrip('#')
    return [int(hex_str[i:i+2], 16) for i in range(0, 6, 2)]

def rgb_para_hex(rgb):
    return '#' + ''.join(f'{int(v):02X}' for v in rgb)

def aumentar_saturacao(hex_str, fator=1.2):
    rgb = hex_para_rgb(hex_str)
    rgb_saturado = [saturar(int(v * fator)) for v in rgb]
    return rgb_para_hex(rgb_saturado)


def change_color(row):
    if row['color_rate']>1:
        return aumentar_saturacao(row['TeamColor'], 1.4)
    return row["TeamColor"]


def get_colors(df):
    df = df.groupby("FullName")["TeamColor"].max().reset_index()
    df['unit'] = 1
    df['color_rate'] = df.groupby("TeamColor")['unit'].cumsum()
    return df.apply(lambda row: change_color(row), axis=1).tolist()
