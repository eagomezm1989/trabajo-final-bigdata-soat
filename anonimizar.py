"""Anonimiza la base de reserva SOAT y la de siniestros.
Llave de cruce: id_poliza = hash con sal secreta de (sucursal | numero de poliza).
Uso:  python anonimizar.py reserva   |   python anonimizar.py siniestros <csv_extraido>
"""
import sys, os, csv, hashlib, secrets, datetime
AQUI = os.path.dirname(os.path.abspath(__file__))
SAL = os.path.join(AQUI, "sal_anonimizacion.txt")       # NO subir a GitHub ni a AWS
if not os.path.exists(SAL):
    open(SAL, "w").write(secrets.token_hex(32))
KEY = bytes.fromhex(open(SAL).read().strip())

def h(*partes):
    txt = "|".join(str(p).strip() for p in partes).encode()
    return hashlib.blake2b(txt, key=KEY, digest_size=8).hexdigest().encode()

def reserva():
    src = os.path.join(AQUI, "Reserva del Mes de DICIEMBRE SOAT.txt")
    dst = os.path.join(AQUI, "datos_anonimizados", "reserva_soat_202512_anon.txt")
    n = 0
    with open(src, "rb") as f, open(dst, "wb", buffering=1 << 24) as g:
        cab = f.readline().rstrip(b"\r\n").split(b"~")
        cab[5] = b"ID_POLIZA"; del cab[7]                 # POLIZA -> ID_POLIZA, se elimina ASEGURADO
        g.write(b"~".join(cab) + b"\n")
        for linea in f:
            p = linea.rstrip(b"\r\n").split(b"~", 8)      # 0 moneda,1 sucursal,...,5 poliza,6 endoso,7 asegurado
            p[5] = h(p[1].decode(), p[5].decode()); del p[7]
            g.write(b"~".join(p) + b"\n"); n += 1
    print("reserva filas:", n)

def fecha(x):
    try: return (datetime.datetime(1899, 12, 30) + datetime.timedelta(days=float(x))).strftime("%Y-%m-%d")
    except Exception: return ""

def siniestros(src):
    dst = os.path.join(AQUI, "datos_anonimizados", "siniestros_soat_202512_anon.csv")
    n = 0
    with open(src, encoding="utf8") as f, open(dst, "w", newline="", encoding="utf8") as g:
        w = None
        for r in csv.DictReader(f, delimiter="~"):
            if r["cod_ramo_t"] != "8": continue           # solo SOAT
            o = {"id_poliza": h(r["cod_suc_prod"], r["nro_pol"]).decode(),
                 "id_siniestro": h("stro", r["cod_suc_stro"], r["nro_stro"]).decode(),
                 "sucursal": r["cod_suc_prod"], "nombre_sucursal": r["txt_nom_suc"],
                 "fecha_ocurrencia": fecha(r["fec_hora_reclamo"]), "fecha_aviso": fecha(r["fec_aviso"]),
                 "fecha_registro": fecha(r["fec_registro"]), "amparo": r["amparo"], "cod_estado": r["cod_estado"],
                 "pago_previsora": r["pago_previsora"] or 0, "reserva_100_actual": r["reserva100_act"] or 0,
                 "reserva_retenida_actual": r["reserva_ret_act"] or 0, "siniestros_incurridos": r["stros_incurridos"] or 0}
            if w is None: w = csv.DictWriter(g, fieldnames=list(o), delimiter="~"); w.writeheader()
            w.writerow(o); n += 1
    print("siniestros filas:", n)

if __name__ == "__main__":
    reserva() if sys.argv[1] == "reserva" else siniestros(sys.argv[2])
