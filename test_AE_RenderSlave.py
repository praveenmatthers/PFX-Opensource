import unittest
from unittest.mock import patch, MagicMock
from AE_RenderSlave import get_local_ip

class TestGetLocalIp(unittest.TestCase):
    @patch('AE_RenderSlave.socket.socket')
    def test_get_local_ip_success(self, mock_socket):
        mock_sock_instance = MagicMock()
        mock_socket.return_value = mock_sock_instance
        mock_sock_instance.getsockname.return_value = ('192.168.1.100', 12345)

        ip = get_local_ip()
        self.assertEqual(ip, '192.168.1.100')

        mock_sock_instance.connect.assert_called_once_with(("8.8.8.8", 80))
        mock_sock_instance.close.assert_called_once()

    @patch('AE_RenderSlave.socket.socket')
    def test_get_local_ip_exception(self, mock_socket):
        mock_socket.side_effect = Exception("Network error")

        ip = get_local_ip()
        self.assertEqual(ip, '127.0.0.1')

if __name__ == '__main__':
    unittest.main()
