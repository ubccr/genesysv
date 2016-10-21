class ElasticSearchFilter():
    def __init__(self):
        self.query_string = {}
        self.size = 1000
        self.source = []

        self.must_term = []
        self.should_term = []
        self.nested_must_term = {}
        self.nested_should_term = {}

        self.must_wildcard = []
        self.nested_must_wildcard = []

        self.must_range_gt = []
        self.must_range_gte = []
        self.must_range_lt = []
        self.must_range_lte = []
        self.nested_must_range_gte = {}

        self.must_exists = []
        self.must_missing = []





    def add_source(self, field_name):
        self.source.append(field_name)

    def get_source(self):
        return self.source

    def add_must_term(self, field_name, value):
        self.must_term.append((field_name, value))

    def get_must_term(self):
        return self.must_term

    def add_should_term(self, field_name, value):
        self.should_term.append((field_name, value))

    def get_should_term(self):
        return self.should_term

    def add_nested_must_term(self, field_name, value, path):
        if path not in self.nested_must_term:
            self.nested_must_term[path] = []
        self.nested_must_term[path].append((field_name, value))

    def get_nested_must_term(self):
        if list(self.nested_must_term):
            return self.nested_must_term
        else:
            None
    def add_nested_should_term(self, field_name, value, path):
        if path not in self.nested_should_term:
            self.nested_should_term[path] = []
        self.nested_should_term[path].append((field_name, value))

    def get_nested_should_term(self):
        if list(self.nested_should_term):
            return self.nested_should_term
        else:
            None

    def add_must_wildcard(self, field_name, value):
        self.must_wildcard.append((field_name, value))

    def get_must_wildcard(self):
        return self.must_wildcard

    def add_nested_must_wildcard(self, field_name, value, path):
        self.nested_must_wildcard.append((field_name, value, path))

    def get_nested_must_wildcard(self):
        return self.nested_must_wildcard

    def add_must_range_gt(self, field_name, value):
        self.must_range_gt.append((field_name, value))

    def get_must_range_gt(self):
        return self.must_range_gt

    def add_must_range_gte(self, field_name, value):
        self.must_range_gte.append((field_name, value))

    def get_must_range_gte(self):
        return self.must_range_gte

    def add_must_range_lt(self, field_name, value):
        self.must_range_lt.append((field_name, value))

    def get_must_range_lt(self):
        return self.must_range_lt

    def add_must_range_lte(self, field_name, value):
        self.must_range_lte.append((field_name, value))

    def get_must_range_lte(self):
        return self.must_range_lte


    def add_nested_must_range_gte(self, field_name, value, path):
        if path not in self.nested_must_range_gte:
            self.nested_must_range_gte[path] = []
        self.nested_must_range_gte[path].append((field_name, value))

    def get_nested_must_range_gte(self):
        if list(self.nested_must_range_gte):
            return self.nested_must_range_gte
        else:
            None

    def add_must_exists(self, field_name, value):
        self.must_exists.append((field_name, value))

    def get_must_exists(self):
        return self.must_exists

    def add_must_missing(self, field_name, value):
        self.must_missing.append((field_name, value))

    def get_must_missing(self):
        return self.must_missing

    def generate_query_string(self):
        query_string = {
            "size" : self.size,
            "filter": {
                    "bool" : {}
                }
            }


        if self.get_must_term():
            must_term = self.get_must_term()
            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []

            for field_name, value in must_term:
                query_string["filter"]["bool"]["must"].append({"term" : {field_name: value}})

        if self.get_should_term():
            should_term = self.get_should_term()

            if "should" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["should"] = []

            for field_name, value in should_term:
                query_string["filter"]["bool"]["should"].append({"term" : {field_name: value}})

        if self.get_nested_must_term():
            nested_must_term = self.get_nested_must_term()

            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []


            for path in list(nested_must_term):
                nested = {
                    "nested" : {
                        "path": path,
                        "filter": {
                            "bool": {
                                "must": []
                            }
                        }
                    }
                }

                for field_name, value in nested_must_term[path]:
                    path_fieldname = "%s.%s" %(path, field_name)
                    nested["nested"]["filter"]["bool"]["must"].append({"term" : {path_fieldname: value}})

                query_string["filter"]["bool"]["must"].append(nested)

        if self.get_nested_should_term():
            nested_should_term = self.get_nested_should_term()

            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []


            for path in list(nested_should_term):
                nested = {
                    "nested" : {
                        "path": path,
                        "filter": {
                            "bool": {
                                "should": []
                            }
                        }
                    }
                }


                for field_name, value in nested_should_term[path]:
                    path_fieldname = "%s.%s" %(path, field_name)
                    nested["nested"]["filter"]["bool"]["should"].append({"term" : {path_fieldname: value}})

                query_string["filter"]["bool"]["must"].append(nested)

        if self.get_nested_must_wildcard():
            nested_must_wildcard = self.get_nested_must_wildcard()

            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []

            for field_name, value, path in nested_must_wildcard:
                nested = {"nested" : {"path": path,
                            "filter": {"bool": {"must": []}}}
                        }
                path_fieldname = "%s.%s" %(path, field_name)
                nested["nested"]["filter"]["bool"]["must"].append({"wildcard" : {path_fieldname: '%s' %(value)}})
            query_string["filter"]["bool"]["must"].append(nested)


        if self.get_must_range_gt():
            must_range_gt = self.get_must_range_gt()

            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []

            for field_name, value in must_range_gt:
                query_string["filter"]["bool"]["must"].append({"range" : {field_name: {"gt": value}}})

        if self.get_must_range_gte():
            must_range_gte = self.get_must_range_gte()

            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []

            for field_name, value in must_range_gte:
                query_string["filter"]["bool"]["must"].append({"range" : {field_name: {"gte": value}}})

        if self.get_must_range_lt():
            must_range_lt = self.get_must_range_lt()

            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []

            for field_name, value in must_range_lt:
                query_string["filter"]["bool"]["must"].append({"range" : {field_name: {"lt": value}}})

        if self.get_must_range_lte():
            must_range_lte = self.get_must_range_lte()

            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []

            for field_name, value in must_range_lte:
                query_string["filter"]["bool"]["must"].append({"range" : {field_name: {"lte": value}}})


        if self.get_nested_must_range_gte():
            nested_must_range_gte = self.get_nested_must_range_gte()

            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []


            for path in list(nested_must_range_gte):
                nested = {
                    "nested" : {
                        "path": path,
                        "filter": {
                            "bool": {
                                "must": []
                            }
                        }
                    }
                }


                for field_name, value in nested_must_range_gte[path]:
                    path_fieldname = "%s.%s" %(path, field_name)
                    nested["nested"]["filter"]["bool"]["must"].append({"range" : {path_fieldname: {"gte": value}}})

                    query_string["filter"]["bool"]["must"].append(nested)


        if self.get_must_exists():
            must_exists = self.get_must_exists()
            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []

            for field_name, value in must_exists:
                query_string["filter"]["bool"]["must"].append({"exists" : {"field": field_name}})

        if self.get_must_missing():
            must_missing = self.get_must_missing()
            if "must" not in query_string["filter"]["bool"]:
                query_string["filter"]["bool"]["must"] = []

            for field_name, value in must_missing:
                query_string["filter"]["bool"]["must"].append({"missing" : {"field": field_name}})

        if self.get_source():
            query_string['_source'] = self.get_source()

        return query_string
