using FileApi.Storage;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace FileApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class FileController : ControllerBase
    {
        const string cs = "mongodb://localhost/GridFsTest";

        [HttpPost("upload/{id}/{consumer}")]
        public async Task UploadFile(int id, string consumer)
        {
            var collection = new FileStorage(cs);
            await collection.SaveFile(id, consumer, Request.Body);
        }

        [HttpGet("download/{id}/{consumer}")]
        public async Task<IActionResult> DownloadFile(int id, string consumer)
        {
            var collection = new FileStorage(cs);

            await collection.ReadFile(id, consumer, Response.Body);
            return Ok();
        }

        [HttpPost("uploadzip/{id}/{consumer}")]
        public async Task UploadZipFile(int id, string consumer)
        {
            var collection = new FileStorage(cs);
            await collection.SaveFileZip(id, consumer, Request.Body);
        }

        [HttpGet]
        public string Test()
        {
            return "OK";
        }

        [HttpGet("downloadzip/{id}/{consumer}")]
        public async Task<IActionResult> DownloadZipFile(int id, string consumer)
        {
            var collection = new FileStorage(cs);
            await collection.ReadFileZip(id, consumer, Response.Body);

            return Ok();
        }
    }
}
