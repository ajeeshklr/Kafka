using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer
{
    public class CryptoUtil
    {

        RijndaelManaged rijAlg = new RijndaelManaged();
        static RSACryptoServiceProvider provider = new RSACryptoServiceProvider();

        public CryptoUtil(string keyPath)
        {
            provider.FromXmlString(File.ReadAllText(keyPath));
        }

        public string DecryptData(byte[] data, byte[] key, byte[] iv)
        {
            string plaintext;

            // Create a decryptor to perform the stream transform.
            ICryptoTransform decryptor = rijAlg.CreateDecryptor(key, iv);

            // Create the streams used for decryption.
            using (MemoryStream msDecrypt = new MemoryStream(data))
            {
                //msDecrypt.Position = 0;
                using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                {
                    //csDecrypt.FlushFinalBlock();

                    using (StreamReader srDecrypt = new StreamReader(csDecrypt, Encoding.UTF8))
                    {
                        // Read the decrypted bytes from the decrypting stream
                        // and place them in a string.
                        plaintext = srDecrypt.ReadToEnd();
                    }
                }
            }

            return plaintext;
        }


        /// <summary>
        /// Decrypt this message using this key
        /// </summary>
        /// <param name="dataToDecrypt">
        /// The data To decrypt.
        /// </param>
        /// <param name="privateKeyInfo">
        /// The private Key Info.
        /// </param>
        /// <returns>
        /// The decrypted data.
        /// </returns>
        public byte[] Decrypt(string str)
        {
            int blockSize = BlockSize;
            //// The bytearray to hold all of our data after decryption
            byte[] decryptedBytes = { };

            //Create a new instance of RSACryptoServiceProvider.
            try
            {
                byte[] bytesToDecrypt = Convert.FromBase64String(str);
                byte[] buffer = new byte[blockSize];

                //// buffer containing decrypted information
                byte[] decryptedBuffer = new byte[blockSize];

                //// Initializes our array to make sure it can hold at least the amount needed to decrypt.
                int totalBufferLength = bytesToDecrypt.Length;
                int bytesAfterDecryption = (((totalBufferLength / blockSize) - 1) * (blockSize - 32)) + blockSize; // By this calculation, we don't have to allocate too much memory.
                decryptedBytes = new byte[bytesAfterDecryption];
                int offset = 0;

                for (int i = 0; i < bytesToDecrypt.Length; i += blockSize)
                {
                    if (i + blockSize > bytesToDecrypt.Length)
                    {
                        blockSize = bytesToDecrypt.Length - i;
                        buffer = new byte[blockSize];
                    }

                    Buffer.BlockCopy(bytesToDecrypt, i, buffer, 0, blockSize);
                    decryptedBuffer = RSADecrypt(buffer, false);
                    decryptedBuffer.CopyTo(decryptedBytes, offset);
                    offset += decryptedBuffer.Length;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }


            return decryptedBytes;

        }

        public byte[] RSADecrypt(byte[] DataToDecrypt, bool DoOAEPPadding)
        {
            try
            {
                return provider.Decrypt(DataToDecrypt, DoOAEPPadding);
            }
            //Catch and display a CryptographicException  
            //to the console.
            catch (CryptographicException e)
            {
                Console.WriteLine(e.ToString());

                return null;
            }
        }


        public byte[] EncryptData(string data)
        {
            byte[] encrypted;
            // Create an RijndaelManaged object
            // with the specified key and IV.

            // Create the streams used for encryption.
            using (MemoryStream msEncrypt = new MemoryStream())
            {
                using (ICryptoTransform encryptor = rijAlg.CreateEncryptor(rijAlg.Key, rijAlg.IV))
                {

                    using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                    {
                        using (StreamWriter swEncrypt = new StreamWriter(csEncrypt, Encoding.UTF8))
                        {
                            //Write all data to the stream.
                            swEncrypt.Write(data);
                            swEncrypt.Close();
                        }
                        encrypted = msEncrypt.ToArray();
                    }
                }
                msEncrypt.Close();
            }

            return encrypted;
        }

        public byte[] RSAEncrypt(byte[] DataToEncrypt, RSAParameters RSAKeyInfo, bool DoOAEPPadding)
        {
            try
            {
                byte[] encryptedData;
                //Create a new instance of RSACryptoServiceProvider.
                using (RSACryptoServiceProvider RSA = new RSACryptoServiceProvider())
                {

                    //Import the RSA Key information. This only needs
                    //toinclude the public key information.
                    RSA.ImportParameters(RSAKeyInfo);

                    //Encrypt the passed byte array and specify OAEP padding.  
                    //OAEP padding is only available on Microsoft Windows XP or
                    //later.  
                    encryptedData = RSA.Encrypt(DataToEncrypt, DoOAEPPadding);
                }
                return encryptedData;
            }
            //Catch and display a CryptographicException  
            //to the console.
            catch (CryptographicException e)
            {
                Console.WriteLine(e.Message);

                return null;
            }
        }

        public byte[] Key { get { return rijAlg.Key; } set { rijAlg.Key = value; } }
        public byte[] IV { get { return rijAlg.IV; } set { rijAlg.IV = value; } }
        public int BlockSize => provider.KeySize / 8;

        public void SetMode(CipherMode mode) => rijAlg.Mode = mode;

        public void SetPadding(PaddingMode mode) => rijAlg.Padding = mode;

        public byte[] Encrypt(byte[] encryptThis)
        {
            try
            {

                int encryptedBufferSize = (provider.KeySize / 8);
                int blockSize = encryptedBufferSize - 32;

                //// buffer to write byte sequence of the given block_size
                byte[] buffer = new byte[blockSize];

                byte[] encryptedBuffer = new byte[encryptedBufferSize];

                var totalBufferLength = ((encryptThis.Length / blockSize) * encryptedBufferSize) + ((encryptThis.Length % blockSize) > 0 ? encryptedBufferSize : 0);


                //// Initializing our encryptedBytes array to a suitable size, depending on the size of data to be encrypted
                byte[] encryptedBytes = new byte[totalBufferLength];
                int offset = 0;

                for (int i = 0; i < encryptThis.Length; i += blockSize)
                {
                    if (i + blockSize > encryptThis.Length)
                    {
                        blockSize = encryptThis.Length - i;
                        buffer = new byte[blockSize];
                    }

                    //// encrypt the specified size of data, then add to final array.
                    Buffer.BlockCopy(encryptThis, i, buffer, 0, blockSize);
                    encryptedBuffer = RSAEncrypt(buffer, provider.ExportParameters(false), false);
                    encryptedBuffer.CopyTo(encryptedBytes, offset);
                    offset += encryptedBufferSize;
                }

                return encryptedBytes;
            }
            catch (CryptographicException e)
            {
                Console.WriteLine(e);
                return null;
            }
        }
    }
}
