class Columns:
    def __init__(self):
        self._columns = [
            "unique_id",
            "price",
            "date_of_transfer",
            "post_code",
            "property_type",
            "is_new_build",
            "estate_type",
            "paon",
            "saon",
            "street",
            "locality",
            "town",
            "district",
            "county",
            "category_type",
            "record_status",
        ]

    def names(self):
        return self._columns
