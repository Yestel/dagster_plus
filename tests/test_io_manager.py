import pickle
import unittest
from unittest.mock import MagicMock, patch

from dagster import InputContext, OutputContext
from crawler.defs.resources.io_manager import S3IOManager


class TestS3IOManager(unittest.TestCase):
    def setUp(self):
        self.bucket = "test-bucket"
        self.prefix = "test-prefix"
        self.io_manager = S3IOManager(s3_bucket=self.bucket, s3_prefix=self.prefix)
        # Mock the s3 client
        self.io_manager.s3 = MagicMock()

    def test_handle_output(self):
        context = MagicMock(spec=OutputContext)
        context.asset_key.path = ["my_asset"]
        context.log = MagicMock()
        
        obj = {"data": 123}
        self.io_manager.handle_output(context, obj)

        expected_key = "test-prefix/my_asset"
        self.io_manager.s3.put_object.assert_called_once()
        call_args = self.io_manager.s3.put_object.call_args
        self.assertEqual(call_args.kwargs["Bucket"], self.bucket)
        self.assertEqual(call_args.kwargs["Key"], expected_key)
        self.assertEqual(pickle.loads(call_args.kwargs["Body"]), obj)

    def test_load_input(self):
        context = MagicMock(spec=InputContext)
        context.asset_key.path = ["my_asset"]
        context.log = MagicMock()

        mock_body = MagicMock()
        mock_body.read.return_value = pickle.dumps({"data": 123})
        self.io_manager.s3.get_object.return_value = {"Body": mock_body}

        result = self.io_manager.load_input(context)

        expected_key = "test-prefix/my_asset"
        self.io_manager.s3.get_object.assert_called_once_with(Bucket=self.bucket, Key=expected_key)
        self.assertEqual(result, {"data": 123})

if __name__ == "__main__":
    unittest.main()
