# Xubuntu 17.10 安装记录

### 进入不了安装界面的解决办法
在最初的安装菜单界面中，按F6并选中nomodeset，可以解决不能识别显卡导致安装界面进不去的问题。

### nvidia gt630 显卡驱动安装
* 参考 [http://blog.csdn.net/10km/article/details/61191230](http://blog.csdn.net/10km/article/details/61191230)
* 由于电脑太旧，所以安装的304版本的驱动 `sudo apt-get install nvidia-304`

### 系统时间与win7同步
* 参考 [http://blog.csdn.net/B__T__T/article/details/71856797](http://blog.csdn.net/B__T__T/article/details/71856797)
* `timedatectl set-local-rtc true`

### grub菜单中默认选中win7
* 修改/etc/default/grub中 `GRUB_DEFAULT=4` 其中数字4是win7在grub菜单中的序号(从0开始)
* `sudo update-grub`更新/boot/grub/grub.cfg
