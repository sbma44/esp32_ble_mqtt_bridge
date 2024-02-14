import functools, json, filecmp, os

def enable_fixtures(cls):
    """
    A decorator that adds the compare_to_fixture method to a class.
    The compare_to_fixture method compares an object's attribute values
    to those found in a fixture file (JSON format assumed for this example).
    """

    def compare_to_fixture(self, obj, fixture_path, obj_is_file_path=False):
        update_fixtures = int(os.getenv('UPDATE', '0')) == 1

        if os.path.dirname(os.path.abspath(__file__)) not in fixture_path:
            fixture_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), fixture_path)

        if update_fixtures:
            if not obj_is_file_path:
                with open(fixture_path, 'w') as f:
                    json.dump(obj, f, indent=2)
            else:
                with open(obj, 'rb') as src:
                    with open(fixture_path, 'wb') as dst:
                        dst.write(src.read())
        else:
            if not obj_is_file_path:
                with open(fixture_path, 'r') as f:
                    self.assertEqual(json.loads(json.dumps(obj)), json.load(f))
            else:
                with open(fixture_path, 'rb') as f:
                    self.assertTrue(filecmp.cmp(obj, fixture_path))

    # Use functools.update_wrapper to preserve metadata
    functools.update_wrapper(compare_to_fixture, cls.compare_to_fixture if hasattr(cls, 'compare_to_fixture') else cls.__init__)
    cls.compare_to_fixture = compare_to_fixture

    return cls


class Accumulator(object):
    def __init__(self):
        self.value = 0

    def get(self):
        self.value = (self.value + 1) % 100
        return self.value