use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::prelude::*;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// This enum represents the possible return types of send_byond
/// It can be nothing, a String (containing String), or a Number (containing f32)
pub enum ByondTopicValue {
    None,
    String(String),
    Number(f32),
}

/// Main (and only) function of this library.
///
/// # Arguments
///
/// * `target` - A TCP SocketAddr of a Dream Daemon instance.
/// * `topic` - The string you want sent to Dream Daemon. Make sure to always start this with the character `?`.ByondTopicValue
/// * `timeout` - Connect/read/write timeout. Defaults to 5 seconds.
///
/// # Examples
///
/// ```
/// use http2byond::{send_byond, ByondTopicValue};
/// use std::net::SocketAddr;
/// match send_byond(&SocketAddr::from(([127, 0, 0, 1], 27012)), "?status", None) {
///     Err(_) => {}
///     Ok(btv_result) => {
///         match btv_result {
///             ByondTopicValue::None => println!("Byond returned nothing"),
///             ByondTopicValue::String(str) => println!("Byond returned string {}", str),
///             ByondTopicValue::Number(num) => println!("Byond returned number {}", num),
///         }
///     }
/// }
/// ```
pub fn send_byond(
    target: &SocketAddr,
    topic: &str,
    timeout: Option<Duration>,
) -> std::io::Result<ByondTopicValue> {
    let timeout = timeout.unwrap_or(Duration::new(5, 0));
    let mut stream = TcpStream::connect_timeout(target, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let topic_bytes = topic.as_bytes();

    let mut buf = BytesMut::with_capacity(1024);
    // Header of 00 83
    buf.put_u16(0x0083);

    // Unsigned short of data length
    buf.put_u16(topic_bytes.len() as u16 + 6);

    // 40 bytes of padding
    buf.put_u32(0x0);
    buf.put_u8(0x0);

    // Append our topic
    buf.put(topic_bytes);

    // End with a 00
    buf.put_u8(0x0);

    stream.write_all(&buf)?;

    // Receive response: 4 byte header (00 83, then u16 big-endian body length),
    // then the body, which may arrive split across multiple reads
    let mut recv_buf = vec![0; 4];
    let bytes_read = stream.read(&mut recv_buf)?;

    if bytes_read == 0 {
        return Ok(ByondTopicValue::None);
    }

    stream.read_exact(&mut recv_buf[bytes_read..])?;
    let size = u16::from_be_bytes([recv_buf[2], recv_buf[3]]) as usize;
    recv_buf.resize(4 + size, 0);
    stream.read_exact(&mut recv_buf[4..])?;

    let mut recv_buf = Bytes::from(recv_buf);

    if recv_buf.try_get_u16()? == 0x0083 {
        let mut size = recv_buf.try_get_u16()? - 1;
        let data_type = recv_buf.try_get_u8()?;

        let ret = match data_type {
            0x2a => ByondTopicValue::Number(recv_buf.try_get_f32_le()?),
            0x06 => {
                let mut str = String::new();
                while size > 0 {
                    str.push(recv_buf.try_get_u8()? as char);
                    size -= 1;
                }
                ByondTopicValue::String(str)
            }
            _ => ByondTopicValue::None,
        };

        return Ok(ret);
    }

    Ok(ByondTopicValue::None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let res = send_byond(&SocketAddr::from(([127, 0, 0, 1], 27012)), "?status", None);
        match res {
            Err(x) => panic!("Error from send_byond {x}"),
            Ok(wrapper) => match wrapper {
                ByondTopicValue::None => println!("Returned NONE"),
                ByondTopicValue::String(s) => println!("Returned string {s}"),
                ByondTopicValue::Number(num) => println!("Returned f32 {num}"),
            },
        }
    }

    #[test]
    fn reads_split_response() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let payload = "x".repeat(10_000);

        let expected = payload.clone();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = stream.read(&mut [0; 64]).unwrap();

            let mut res = BytesMut::new();
            res.put_u16(0x0083);
            res.put_u16(payload.len() as u16 + 1);
            res.put_u8(0x06);
            res.put(payload.as_bytes());

            for chunk in res.chunks(1000) {
                stream.write_all(chunk).unwrap();
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        match send_byond(&addr, "?status", Some(Duration::from_secs(2))).unwrap() {
            ByondTopicValue::String(s) => assert_eq!(s, expected),
            _ => panic!("expected string"),
        }
    }
}
