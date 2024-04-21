import structlog

log = structlog.get_logger()


def main():
    args = {"a": 1, "b": 2}
    log.info("Hello from hi.py", args=args)
    # print(1 / 0)


if __name__ == "__main__":
    main()
