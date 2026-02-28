def test_func():
    try:
        raise ValueError("test")
    except Exception as e:
        raise
    finally:
        return 42

print(test_func())
