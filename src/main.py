import argparse
from events import despesas, contratos, notas_fiscais, cpgf, integridade

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cnpj", required=True)
    ap.add_argument("--familia", choices=["despesas","contratos","nfe","cpgf","sancoes","all"], default="all")
    ap.add_argument("--start", default=None)
    args = ap.parse_args()

    if args.familia in ("despesas","all"):
        despesas.run(args.cnpj, args.start) if args.start else despesas.run(args.cnpj)
    if args.familia in ("contratos","all"):
        contratos.run(args.cnpj)
    if args.familia in ("nfe","all"):
        notas_fiscais.run(args.cnpj, args.start) if args.start else notas_fiscais.run(args.cnpj)
    if args.familia in ("cpgf","all"):
        cpgf.run(args.cnpj, args.start) if args.start else cpgf.run(args.cnpj)
    if args.familia in ("sancoes","all"):
        integridade.run(args.cnpj)

if __name__ == "__main__":
    main()
