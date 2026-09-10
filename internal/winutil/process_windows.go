package winutil

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Tree owns both handles. The root starts suspended, is assigned to the job,
// and only then is its initial thread resumed. No user code can escape between
// CreateProcess and AssignProcessToJobObject.
type Tree struct {
	Job, Process windows.Handle
	PID          int
	Identity     string
	Done         <-chan struct{}
}

func StartTree(cmd *exec.Cmd, name string) (*Tree, error) {
	var jobName *uint16
	var err error
	if name != "" {
		jobName, err = windows.UTF16PtrFromString(name)
		if err != nil {
			return nil, err
		}
	}
	sd, err := PrivateDescriptor()
	if err != nil {
		return nil, err
	}
	sa := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
	job, err := windows.CreateJobObject(&sa, jobName)
	if err != nil {
		return nil, err
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		windows.CloseHandle(job)
		return nil, err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_SUSPENDED | windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_NO_WINDOW}
	if err = cmd.Start(); err != nil {
		windows.CloseHandle(job)
		return nil, err
	}
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(cmd.Process.Pid))
	fail := func(e error) (*Tree, error) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if process != 0 {
			windows.CloseHandle(process)
		}
		windows.CloseHandle(job)
		return nil, e
	}
	if err != nil {
		return fail(err)
	}
	if err = windows.AssignProcessToJobObject(job, process); err != nil {
		return fail(fmt.Errorf("contain process tree: %w", err))
	}
	identity, err := HandleProcessIdentity(process)
	if err != nil {
		return fail(err)
	}
	if err = resumeInitialThread(uint32(cmd.Process.Pid)); err != nil {
		return fail(err)
	}
	done := make(chan struct{})
	go func() { _, _ = windows.WaitForSingleObject(process, windows.INFINITE); close(done) }()
	return &Tree{Job: job, Process: process, PID: cmd.Process.Pid, Identity: identity, Done: done}, nil
}

func resumeInitialThread(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot)
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != pid {
			continue
		}
		thread, e := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
		if e != nil {
			return e
		}
		_, e = windows.ResumeThread(thread)
		windows.CloseHandle(thread)
		return e
	}
	return errors.New("suspended process initial thread unavailable")
}
func (t *Tree) Kill() error { return windows.TerminateJobObject(t.Job, 1) }
func (t *Tree) Close() {
	_ = t.Kill()
	<-t.Done
	windows.CloseHandle(t.Process)
	windows.CloseHandle(t.Job)
}

var openJobObject = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")

func KillNamedTree(name string) error {
	p, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return err
	}
	h, _, callErr := openJobObject.Call(0x0008, 0, uintptr(unsafe.Pointer(p)))
	if h == 0 {
		return callErr
	}
	defer windows.CloseHandle(windows.Handle(h))
	return windows.TerminateJobObject(windows.Handle(h), 1)
}

func ProcessIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", errors.New("invalid process id")
	}
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(h)
	return HandleProcessIdentity(h)
}
func HandleProcessIdentity(h windows.Handle) (string, error) {
	var created, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(h, &created, &exited, &kernel, &user); err != nil {
		return "", err
	}
	status, err := windows.WaitForSingleObject(h, 0)
	if err != nil {
		return "", err
	}
	if status != uint32(windows.WAIT_TIMEOUT) {
		return "", errors.New("process has exited")
	}
	return fmt.Sprintf("windows:%08x%08x", created.HighDateTime, created.LowDateTime), nil
}
