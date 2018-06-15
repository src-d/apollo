import argparse
import sys
import unittest


import apollo.__main__ as main


class MainTests(unittest.TestCase):
    def test_handlers(self):
        action2handler = {
            "warmup": "warmup",
            "resetdb": "reset_db",
            "bags": "source2bags",
            "preprocess": "preprocess",
            "hash": "hash_batches",
            "query": "query",
            "cc": "find_connected_components",
            "dumpcc": "dumpcc",
            "cmd": "detect_communities",
            "dumpcmd": "dumpcmd",
            "evalcc": "evaluate_communities",
        }
        parser = main.get_parser()
        subcommands = set([x.dest for x in parser._subparsers._actions[2]._choices_actions])
        set_action2handler = set(action2handler)
        self.assertFalse(len(subcommands - set_action2handler),
                         "You forgot to add to this test {} subcommand(s) check".format(
                             subcommands - set_action2handler))

        self.assertFalse(len(set_action2handler - subcommands),
                         "You cover unexpected subcommand(s) {}".format(
                             set_action2handler - subcommands))

        called_actions = []
        args_save = sys.argv
        error_save = argparse.ArgumentParser.error
        try:
            argparse.ArgumentParser.error = lambda self, message: None

            for action, handler in action2handler.items():
                def handler_append(*args, **kwargs):
                    called_actions.append(action)

                handler_save = getattr(main, handler)
                try:
                    setattr(main, handler, handler_append)
                    sys.argv = [main.__file__, action]
                    main.main()
                finally:
                    setattr(main, handler, handler_save)
        finally:
            sys.argv = args_save
            argparse.ArgumentParser.error = error_save

        set_called_actions = set(called_actions)
        set_actions = set(action2handler)
        self.assertEqual(set_called_actions, set_actions)
        self.assertEqual(len(set_called_actions), len(called_actions))

    def test_args(self):
        handlers = [False] * 10

        def feature_weight_args(args):
            for ex in main.extractors.__extractors__.values():
                self.assertTrue(hasattr(args, "%s_weight" % ex.NAME))

        def cassandra_args(args):
            self.assertTrue(hasattr(args, "cassandra"))
            self.assertTrue(hasattr(args, "keyspace"))
            self.assertTrue(hasattr(args, "tables"))

        def wmh_args(args):
            self.assertTrue(hasattr(args, "params"))
            self.assertTrue(hasattr(args, "threshold"))
            self.assertTrue(hasattr(args, "false_positive_weight"))
            self.assertTrue(hasattr(args, "false_positive_weight"))

        def template_args(args):
            self.assertTrue(hasattr(args, "batch"))
            self.assertTrue(hasattr(args, "template"))

        def spark_args(args):
            self.assertTrue(hasattr(args, "spark"))
            self.assertTrue(hasattr(args, "config"))
            self.assertTrue(hasattr(args, "memory"))
            self.assertTrue(hasattr(args, "packages"))
            self.assertTrue(hasattr(args, "dep_zip"))
            self.assertTrue(hasattr(args, "spark_local_dir"))
            self.assertTrue(hasattr(args, "spark_log_level"))
            self.assertTrue(hasattr(args, "persist"))
            self.assertTrue(hasattr(args, "pause"))
            self.assertTrue(hasattr(args, "explain"))

        def engine_args(args):
            spark_args(args)
            self.assertTrue(hasattr(args, "bblfsh"))
            self.assertTrue(hasattr(args, "engine"))
            self.assertTrue(hasattr(args, "repository_format"))

        def repo2_args(args):
            engine_args(args)
            self.assertTrue(hasattr(args, "repositories"))
            self.assertTrue(hasattr(args, "parquet"))
            self.assertTrue(hasattr(args, "graph"))
            self.assertTrue(hasattr(args, "languages"))

        def bow_args(args):
            self.assertTrue(hasattr(args, "bow"))
            self.assertTrue(hasattr(args, "batch"))

        def dzhigurda_args(args):
            self.assertTrue(hasattr(args, "dzhigurda"))

        def feature_args(args):
            self.assertTrue(hasattr(args, "mode"))
            self.assertTrue(hasattr(args, "quant"))
            self.assertTrue(hasattr(args, "feature"))
            for ex in main.extractors.__extractors__.values():
                for opt, val in ex.OPTS.items():
                    self.assertTrue(hasattr(args, "%s_%s" % (ex.NAME, opt.replace("-", "_"))))

        def df_args(args):
            self.assertTrue(hasattr(args, "min_docfreq"))
            self.assertTrue(hasattr(args, "docfreq_out"))
            self.assertTrue(hasattr(args, "docfreq_in"))
            self.assertTrue(hasattr(args, "vocabulary_size"))

        def repartitioner_arg(args):
            self.assertTrue(hasattr(args, "partitions"))
            self.assertTrue(hasattr(args, "shuffle"))

        def cached_index_arg(args):
            self.assertTrue(hasattr(args, "cached_index_path"))

        def warmup(args):
            engine_args(args)
            handlers[0] = True

        def reset_db(args):
            cassandra_args(args)
            self.assertTrue(hasattr(args, "hashes_only"))
            handlers[1] = True

        def preprocess(args):
            df_args(args)
            repo2_args(args)
            feature_args(args)
            repartitioner_arg(args)
            cached_index_arg(args)
            handlers[2] = True

        def bags(args):
            bow_args(args)
            dzhigurda_args(args)
            repo2_args(args)
            feature_args(args)
            cassandra_args(args)
            df_args(args)
            repartitioner_arg(args)
            cached_index_arg(args)
            handlers[3] = True

        def hasher(args):
            self.assertTrue(hasattr(args, "input"))
            self.assertTrue(hasattr(args, "seed"))
            self.assertTrue(hasattr(args, "mhc_verbosity"))
            self.assertTrue(hasattr(args, "devices"))
            self.assertTrue(hasattr(args, "size"))
            wmh_args(args)
            cassandra_args(args)
            spark_args(args)
            feature_weight_args(args)
            repartitioner_arg(args)
            handlers[4] = True

        def query(args):
            self.assertTrue(hasattr(args, "id"))
            self.assertTrue(hasattr(args, "file"))
            self.assertTrue(hasattr(args, "docfreq"))
            self.assertTrue(hasattr(args, "min_docfreq"))
            self.assertTrue(hasattr(args, "bblfsh"))
            self.assertTrue(hasattr(args, "precise"))
            wmh_args(args)
            feature_args(args)
            template_args(args)
            cassandra_args(args)
            handlers[5] = True

        def cc(args):
            self.assertTrue(hasattr(args, "output"))
            cassandra_args(args)
            handlers[6] = True

        def dumpcc(args):
            self.assertTrue(hasattr(args, "input"))
            handlers[7] = True

        def cmd(args):
            self.assertTrue(hasattr(args, "input"))
            self.assertTrue(hasattr(args, "output"))
            self.assertTrue(hasattr(args, "edges"))
            self.assertTrue(hasattr(args, "algorithm"))
            self.assertTrue(hasattr(args, "params"))
            self.assertTrue(hasattr(args, "no_spark"))
            spark_args(args)
            handlers[8] = True

        def dumpcmd(args):
            self.assertTrue(hasattr(args, "input"))
            template_args(args)
            cassandra_args(args)
            handlers[9] = True

        def evalcc(args):
            self.assertTrue(hasattr(args, "input"))
            self.assertTrue(hasattr(args, "threshold"))
            spark_args(args)
            cassandra_args(args)
            handlers[10] = True

        main.warmup = warmup
        main.reset_db = reset_db
        main.source2bags = bags
        main.preprocess = preprocess
        main.hash_batches = hasher
        main.query = query
        main.find_connected_components = cc
        main.dumpcc = dumpcc
        main.detect_communities = cmd
        main.dumpcmd = dumpcmd
        main.evaluate_communities = evalcc
        args = sys.argv
        error = argparse.ArgumentParser.error
        argparse.ArgumentParser.error = lambda self, message: None

        for action in ("warmup", "resetdb", "bags", "hash", "query", "cc", "dumpcc", "cmd",
                       "dumpcmd", "evalcc"):
            sys.argv = [main.__file__, action]
            main.main()

        sys.argv = args
        argparse.ArgumentParser.error = error
        self.assertEqual(sum(handlers), 11)


if __name__ == "__main__":
    unittest.main()
