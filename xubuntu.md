# Xubuntu 17.10 安装记录

### 进入不了安装界面的解决办法
在最初的安装菜单界面中，按F6并选中nomodeset，可以解决不能识别显卡导致安装界面进不去的问题。

### nvidia gt630 显卡驱动安装
参考 http://blog.csdn.net/10km/article/details/61191230
由于电脑太旧，所以安装的304版本的驱动
`sudo apt-get install nvidia-304`

### spark安装准备

* 安装autoconf automake libtool cmake `sudo apt install automake libtool cmake`
* 安装ncurses-devel `sudo apt install libncurses5-dev`
* 安装openssl-devel `sudo apt install libssl-dev`
* 安装maven
  - http://maven.apache.org/download.cgi
* 安装protobuf 2.5.0
  - https://github.com/google/protobuf/releases?after=v2.6.0
