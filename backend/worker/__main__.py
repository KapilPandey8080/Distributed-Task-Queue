# Entrypoint: `python -m worker`.
# The import lives inside the guard on purpose: the task executor starts child processes with
# "spawn", which re-imports this file in every child. Keeping heavy imports behind the guard
# stops each child from loading SQLAlchemy/Redis just to run one small task.
if __name__ == "__main__":
    from worker.runner import main

    main()