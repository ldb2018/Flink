package com.hlhy.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 * @author jacky
 *
 */
public class FileUtils {

	private static InputStream newInputStream(Path path, OpenOption... options) throws IOException {
		return Files.newInputStream(path, options);
	}

	private static OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
		return Files.newOutputStream(path, options);
	}

	private static InputStream newInputStream(File file, OpenOption... options) throws IOException {
		return newInputStream(file.getPath(), options);
	}

	private static OutputStream newOutputStream(File file, OpenOption... options) throws IOException {
		return newOutputStream(file.getPath(), options);
	}

	private static InputStream newInputStream(String path, OpenOption... options) throws IOException {
		return newInputStream(Paths.get(path), options);
	}

	private static OutputStream newOutputStream(String path, OpenOption... options) throws IOException {
		
		return newOutputStream(Paths.get(path), options);
	}

	/**
	 * newReader
	 * 
	 * @throws IOException
	 */
	public static InputStreamReader newReader(InputStream is, Charset charset) throws IOException {
		return new InputStreamReader(is, charset);
	}

	/**
	 * newWriter
	 * 
	 * @throws IOException
	 */
	public static OutputStreamWriter newWriter(OutputStream os, Charset charset) throws IOException {
		return new OutputStreamWriter(os, charset);
	}

	/**
	 * newBufferedReader
	 * 
	 * @throws IOException
	 */
	public static BufferedReader newBufferedReader(InputStream is, Charset charset) throws IOException {
		InputStreamReader isr = newReader(is, charset);
		return new BufferedReader(isr);
	}

	/**
	 * newBufferedWriter
	 * 
	 * @throws IOException
	 */
	public static BufferedWriter newBufferedWriter(OutputStream os, Charset charset) throws IOException {
		OutputStreamWriter osw = newWriter(os, charset);
		return new BufferedWriter(osw);
	}

	/**
	 * newReader
	 * 
	 * @throws IOException
	 */
	public static InputStreamReader newReader(Path path, Charset charset) throws IOException {
		InputStream is = newInputStream(path);
		return newReader(is, charset);
	}

	public static InputStreamReader newReader(File file, Charset charset) throws IOException {
		InputStream is = newInputStream(file);
		return newReader(is, charset);
	}

	public static InputStreamReader newReader(String path, Charset charset) throws IOException {
		InputStream is = newInputStream(path);
		return newReader(is, charset);
	}

	public static InputStreamReader newReader(Path path) throws IOException {
		return newReader(path, StandardCharsets.UTF_8);
	}

	public static InputStreamReader newReader(File file) throws IOException {
		return newReader(file, StandardCharsets.UTF_8);
	}

	public static InputStreamReader newReader(String path) throws IOException {
		return newReader(path, StandardCharsets.UTF_8);
	}

	/**
	 * newBufferedReader
	 * 
	 * @throws IOException
	 */
	public static BufferedReader newBufferedReader(Path path, Charset charset) throws IOException {
		InputStream is = newInputStream(path);
		return newBufferedReader(is, charset);
	}

	public static BufferedReader newBufferedReader(File file, Charset charset) throws IOException {
		InputStream is = newInputStream(file);
		return newBufferedReader(is, charset);
	}

	public static BufferedReader newBufferedReader(String path, Charset charset) throws IOException {
		InputStream is = newInputStream(path);
		return newBufferedReader(is, charset);
	}

	public static BufferedReader newBufferedReader(Path path) throws IOException {
		return newBufferedReader(path, StandardCharsets.UTF_8);
	}

	public static BufferedReader newBufferedReader(File file) throws IOException {
		return newBufferedReader(file, StandardCharsets.UTF_8);
	}

	public static BufferedReader newBufferedReader(String path) throws IOException {
		return newBufferedReader(path, StandardCharsets.UTF_8);
	}

	/**
	 * newWriter
	 * 
	 * @throws IOException
	 */
	public static OutputStreamWriter newWriter(Path path, Charset charset, OpenOption... options) throws IOException {
		OutputStream os = newOutputStream(path, options);
		return newWriter(os, charset);
	}

	public static OutputStreamWriter newWriter(File file, Charset charset, OpenOption... options) throws IOException {
		OutputStream os = newOutputStream(file, options);
		return newWriter(os, charset);
	}

	public static OutputStreamWriter newWriter(String path, Charset charset, OpenOption... options) throws IOException {
		OutputStream os = newOutputStream(path, options);
		return newWriter(os, charset);
	}

	public static OutputStreamWriter newWriter(Path path, OpenOption... options) throws IOException {
		return newWriter(path, StandardCharsets.UTF_8, options);
	}

	public static OutputStreamWriter newWriter(File file, OpenOption... options) throws IOException {
		return newWriter(file, StandardCharsets.UTF_8, options);
	}

	public static OutputStreamWriter newWriter(String path, OpenOption... options) throws IOException {
		return newWriter(path, StandardCharsets.UTF_8, options);
	}

	/**
	 * newBufferedWriter
	 * 
	 * @throws IOException
	 */
	public static BufferedWriter newBufferedWriter(Path path, Charset charset, OpenOption... options) throws IOException {
		OutputStream os = newOutputStream(path, options);
		return newBufferedWriter(os, charset);
	}

	public static BufferedWriter newBufferedWriter(File file, Charset charset, OpenOption... options) throws IOException {
		OutputStream os = newOutputStream(file, options);
		return newBufferedWriter(os, charset);
	}

	public static BufferedWriter newBufferedWriter(String path, Charset charset, OpenOption... options) throws IOException {
		OutputStream os = newOutputStream(path, options);
		return newBufferedWriter(os, charset);
	}

	public static BufferedWriter newBufferedWriter(Path path, OpenOption... options) throws IOException {
		return newBufferedWriter(path, StandardCharsets.UTF_8, options);
	}

	public static BufferedWriter newBufferedWriter(File file, OpenOption... options) throws IOException {
		return newBufferedWriter(file, StandardCharsets.UTF_8, options);
	}

	public static BufferedWriter newBufferedWriter(String path, OpenOption... options) throws IOException {
		createPath(path);
		return newBufferedWriter(path, StandardCharsets.UTF_8, options);
	}
	
	public static void createPath(String path) throws IOException {
		File file = new File(path);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if(!file.exists()){
        	file.createNewFile();
        }
	}
	
	public static void createDir(String path) throws IOException {
		File file = new File(path);
        file.mkdirs();
	}
	
	public static File createNewFile(String path, OpenOption... options) throws IOException {

		File file = new File(path);
		File fileParent = file.getParentFile();
		if(!fileParent.exists()) {
			fileParent.mkdirs();
			
		}
		
		file.createNewFile();
		return file;
	}
	
	/**
	 * 获取指定目录下的所有文件(不包括子文件夹)
	 *
	 * @param dirPath
	 * @return
	 */
	public static ArrayList<File> getDirFiles(String dirPath) {
		File path = new File(dirPath);
		File[] fileArr = path.listFiles();
		ArrayList<File> files = new ArrayList<File>();

		for (File f : fileArr) {
			if (f.isFile()) {
				files.add(f);
			}
		}
		return files;
	}


	/**
	 * 获取指定目录下特定文件后缀名的文件列表(不包括子文件夹)
	 *
	 * @param dirPath
	 *            目录路径
	 * @param suffix
	 *            文件后缀
	 * @return
	 */
	public static ArrayList<File> getDirFiles(String dirPath,
											  final String suffix) {
		File path = new File(dirPath);
		File[] fileArr = path.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				String lowerName = name.toLowerCase();
				String lowerSuffix = suffix.toLowerCase();
				if (lowerName.endsWith(lowerSuffix)) {
					return true;
				}
				return false;
			}

		});
		ArrayList<File> files = new ArrayList<File>();

		for (File f : fileArr) {
			if (f.isFile()) {
				files.add(f);
			}
		}
		return files;
	}


	/**
	 * 拆分文件
	 *
	 * @param fileName
	 *            待拆分的完整文件名
	 * @param byteSize
	 *            按多少字节大小拆分
	 * @return 拆分后的文件名列表
	 * @throws IOException
	 */
	public static List<String> splitBySize(String fileName, int byteSize)
			throws IOException {
		List<String> parts = new ArrayList<String>();
		File file = new File(fileName);
		String parentName =file.getParent().toString();
		String  filename=file.getName().toString().split("\\.")[0];
		String  filepath=new File(parentName, filename).toString();
		new File(filepath).mkdirs();
		int count = (int) Math.ceil(file.length() / (double) byteSize);
		int countLen = (count + "").length();
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(count,
				count * 3, 1, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(count * 2));

		for (int i = 0; i < count; i++) {
			String partFileName = new File(filepath, file.getName()).toString() +"."
					+ leftPad((i + 1) + "", countLen, '0') + ".part";
			threadPool.execute(new SplitRunnable(byteSize, i * byteSize,
					partFileName, file));
			parts.add(partFileName);
		}
		return parts;
	}
	
    /**
     * 当前目录路径
     */
    public static String currentWorkDir = System.getProperty("user.dir") + "\\";

    /**
     * 左填充
     * 
     * @param str
     * @param length
     * @param ch
     * @return
     */
    public static String leftPad(String str, int length, char ch) {
        if (str.length() >= length) {
            return str;
        }
        char[] chs = new char[length];
        Arrays.fill(chs, ch);
        char[] src = str.toCharArray();
        System.arraycopy(src, 0, chs, length - src.length, src.length);
        return new String(chs);

    }



	/**
	 * 根据文件名，比较文件
	 *
	 * @author yjmyzz@126.com
	 *
	 */
	private class FileComparator implements Comparator<File> {
		public int compare(File o1, File o2) {
			return o1.getName().compareToIgnoreCase(o2.getName());
		}
	}

	/**
	 * 分割处理Runnable
	 *
	 * @author yjmyzz@126.com
	 *
	 */
	private static class SplitRunnable implements Runnable {
		int byteSize;
		String partFileName;
		File originFile;
		int startPos;

		public SplitRunnable(int byteSize, int startPos, String partFileName,
							 File originFile) {
			this.startPos = startPos;
			this.byteSize = byteSize;
			this.partFileName = partFileName;
			this.originFile = originFile;
		}

		public void run() {
			RandomAccessFile rFile;
			OutputStream os;
			try {
				rFile = new RandomAccessFile(originFile, "r");
				byte[] b = new byte[byteSize];
				rFile.seek(startPos);// 移动指针到每“段”开头
				int s = rFile.read(b); ////按文件大小一次读入
				os = new FileOutputStream(partFileName);
				os.write(b, 0, s);
				os.flush();
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	//1、利用BufferedWriter进行写操作，写入的内容会放在缓冲区内，直到遇到close()、flush()的时候才会将内容一次性写入文件。
	//另外注意close()的顺序，一定要先关闭BufferedWriter，再关闭Writer，不可以倒过来，因为BufferedWriter的写操作是通过Writer的write方法写的，
	//如果先关闭Writer的话，就无法将缓冲区内的数据写入文件了，会抛出异常。
	public static void main(String[] args) {
//		if (args.length > -1) {
//			//String path = args[0];
//			String path = "E:\\workspace\\public\\src\\com\\pub\\utils\\int.proerties";
//			String out = "E:\\workspace\\public\\src\\com\\pub\\utils\\out.proerties";
//			try (BufferedReader br = newBufferedReader(path);BufferedWriter bw = newBufferedWriter(out,StandardOpenOption.APPEND)) {//表示追加
//				StringBuffer sb = new StringBuffer();
//				try {
//					String line = null;
//					while ((line = br.readLine()) != null) {
//						List<String> fields = StringUtil.split(line);
//						String field1 = fields.get(0); 
//						String field2 = fields.get(1); 
//						String field3 = fields.get(2); 
//						if(field1.endsWith("00")&&field3.endsWith("00")) {
//							bw.write(field1+"\t"+field2+"\t"+"\t"+field3.substring(0, field3.length()-2));
//							bw.write("\n");//写的时候默认是没有换行符的
//						}else {
//							bw.write(field1+"\t"+"\t"+field2+"\t"+field3);
//							bw.write("\n");//写的时候默认是没有换行符的
//						}
//							
//					}
//				} finally {
//					System.out.println(sb);
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
		String path = "E:\\cc.log";
		try {
			List<String> splitBySize = splitBySize(path,10240);
			for (int i = 0; i < splitBySize.size(); i++) {
				System.out.println(splitBySize.get(i));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}