/* What a build gets back from the system calls Nix's sandbox filters
 * (extended attributes: ENOTSUP; setuid/setgid mode changes: EPERM)
 * and from ones it leaves alone (openat2, plain chmod, creating with
 * a setuid mode, getpid through the i386 and x32 ABIs, which a filter
 * without those architectures kills). One "call answer" line each:
 * "ok" or the errno name. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/openat2.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/xattr.h>
#include <unistd.h>

#ifndef SYS_fchmodat2
#define SYS_fchmodat2 452
#endif

static void answer(const char *call, long rc)
{
    printf("%s %s\n", call, rc < 0 ? strerrorname_np(errno) : "ok");
}

/* getpid (20 on i386) through int $0x80, which the kernel clobbers r8-r11 for */
static long i386_getpid(void)
{
    long rc;
    __asm__ volatile("int $0x80" : "=a"(rc) : "a"(20L) : "r8", "r9", "r10", "r11", "memory");
    if (rc < 0) {
        errno = (int)-rc;
        return -1;
    }
    return rc;
}

int main(void)
{
    char buf[64];
    int fd = open("f", O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("f");
        return 1;
    }
    struct open_how how = {
        .flags = O_RDONLY | O_DIRECTORY,
        .resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
    };
    answer("openat2", syscall(SYS_openat2, AT_FDCWD, ".", &how, sizeof how));
    answer("setxattr", setxattr("f", "user.t", "v", 1, 0));
    answer("lsetxattr", lsetxattr("f", "user.t", "v", 1, 0));
    answer("fsetxattr", fsetxattr(fd, "user.t", "v", 1, 0));
    answer("getxattr", getxattr("f", "user.t", buf, sizeof buf));
    answer("lgetxattr", lgetxattr("f", "user.t", buf, sizeof buf));
    answer("fgetxattr", fgetxattr(fd, "user.t", buf, sizeof buf));
    answer("listxattr", listxattr("f", buf, sizeof buf));
    answer("llistxattr", llistxattr("f", buf, sizeof buf));
    answer("flistxattr", flistxattr(fd, buf, sizeof buf));
    answer("chmod-setuid", chmod("f", 04755));
    answer("chmod-setgid", chmod("f", 02755));
    answer("fchmod-setuid", fchmod(fd, 04755));
    answer("fchmodat-setgid", fchmodat(AT_FDCWD, "f", 02755, 0));
    answer("fchmodat2-setuid", syscall(SYS_fchmodat2, AT_FDCWD, "f", 04755, 0));
    answer("chmod-plain", chmod("f", 0755));
    answer("creat-setuid", open("g", O_WRONLY | O_CREAT | O_EXCL, 04755));
    answer("mkdir-setgid", mkdir("d", 02755));
    answer("i386-getpid", i386_getpid());
    /* The filter judges it before the kernel: without x32 it kills the
     * build. A kernel without the x32 ABI, as in the test VMs, then
     * answers ENOSYS, so the call itself is not exercised. */
    answer("x32-getpid", syscall(0x40000000 | SYS_getpid));
    return 0;
}
