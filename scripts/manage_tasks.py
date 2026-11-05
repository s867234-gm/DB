# -*- coding: utf-8 -*-
"""
scripts/manage_tasks.py —— Windows 计划任务自动化注册与生命周期管理工具

支持命令:
    python scripts/manage_tasks.py install     # 注册盘中 16 节点与盘后 15:32 两个定时任务
    python scripts/manage_tasks.py uninstall   # 卸载移除定时任务
    python scripts/manage_tasks.py status      # 查询定时任务当前运行状态与下次触发时间
"""

import argparse
import datetime
import getpass
import os
import subprocess
import sys
import tempfile
from typing import List, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_SCRIPT = os.path.join(PROJECT_ROOT, "scripts", "run_daily_update.py")
PYTHON_EXE = sys.executable

TASK_INTRADAY = "A_DB_Intraday_15m_Update"
TASK_POST_MARKET = "A_DB_PostMarket_Daily_Update"

INTRADAY_TIMES = [
    # 上午 8 个 15 分钟节点 (完结后第 1 分钟自动触发)
    "09:46:00",
    "10:01:00",
    "10:16:00",
    "10:31:00",
    "10:46:00",
    "11:01:00",
    "11:16:00",
    "11:31:00",
    # 下午 8 个 15 分钟节点 (完结后第 1 分钟自动触发)
    "13:16:00",
    "13:31:00",
    "13:46:00",
    "14:01:00",
    "14:16:00",
    "14:31:00",
    "14:46:00",
    "15:01:00",
]

POST_MARKET_TIME = "15:32:00"


def get_current_user_sid() -> Optional[str]:
    """获取当前登录用户的 Windows SID"""
    try:
        out = subprocess.check_output(["whoami", "/user"], text=True, errors="ignore")
        for line in out.splitlines():
            parts = line.strip().split()
            if len(parts) >= 2 and parts[-1].startswith("S-1-5-"):
                return parts[-1]
    except Exception:
        pass
    return None


def generate_task_xml(
    task_name: str,
    description: str,
    times: List[str],
    arguments: str,
    user_sid: Optional[str] = None,
) -> str:
    """生成符合 Windows Task Scheduler 2.0 规范的 XML 描述文本"""
    author = os.environ.get("USERNAME") or getpass.getuser()
    sid_element = f"<UserId>{user_sid}</UserId>" if user_sid else f"<UserId>{author}</UserId>"

    # 构造触发器列表 (周一至周五 CalendarTrigger)
    trigger_xml_parts = []
    for t in times:
        trigger_xml_parts.append(f"""    <CalendarTrigger>
      <StartBoundary>2026-01-01T{t}</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByWeek>
        <DaysOfWeek>
          <Monday />
          <Tuesday />
          <Wednesday />
          <Thursday />
          <Friday />
        </DaysOfWeek>
        <WeeksInterval>1</WeeksInterval>
      </ScheduleByWeek>
    </CalendarTrigger>""")
    triggers_block = "\n".join(trigger_xml_parts)

    cmd_args = f'/c ""{PYTHON_EXE}" "{RUN_SCRIPT}" {arguments}""'

    xml = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>{datetime.date.today().isoformat()}T00:00:00</Date>
    <Author>{author}</Author>
    <Description>{description}</Description>
    <URI>\\{task_name}</URI>
  </RegistrationInfo>
  <Triggers>
{triggers_block}
  </Triggers>
  <Principals>
    <Principal id="Author">
      {sid_element}
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>true</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT2H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>cmd.exe</Command>
      <Arguments>{cmd_args}</Arguments>
      <WorkingDirectory>{PROJECT_ROOT}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>"""
    return xml


def install_tasks():
    """注册 2 个计划任务"""
    print("=" * 70)
    print("  开始注册 A_DB Windows 计划任务")
    print("=" * 70)
    user_sid = get_current_user_sid()
    print(f"当前用户 SID: {user_sid or '使用用户名默认'}")
    print(f"Python 解释器: {PYTHON_EXE}")
    print(f"执行脚本路径: {RUN_SCRIPT}")

    tasks_cfg = [
        (
            TASK_INTRADAY,
            "A_DB 盘中交易时段每根 15m K 线完结自动增量更新 (周一至周五 09:46~11:31, 13:16~15:01 共 16 次)",
            INTRADAY_TIMES,
            "--allow-intraday --workers 24 --batch-size 500",
        ),
        (
            TASK_POST_MARKET,
            "A_DB 盘后 15:32 完整日线与分钟线增量归档更新 (周一至周五 15:32)",
            [POST_MARKET_TIME],
            "--workers 24 --batch-size 500",
        ),
    ]

    for t_name, desc, times, args in tasks_cfg:
        xml_content = generate_task_xml(t_name, desc, times, args, user_sid=user_sid)
        temp_fd, temp_path = tempfile.mkstemp(suffix=".xml", text=False)
        try:
            with open(temp_path, "w", encoding="utf-16") as f:
                f.write(xml_content)
            os.close(temp_fd)

            cmd = ["schtasks", "/Create", "/TN", t_name, "/XML", temp_path, "/F"]
            proc = subprocess.run(cmd, capture_output=True)
            if proc.returncode == 0:
                print(f"[成功] 计划任务注册成功: {t_name}")
            else:
                err_msg = proc.stderr.decode("gbk", errors="ignore").strip()
                print(f"[失败] 计划任务 {t_name} 注册失败: {err_msg}")
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    print()
    status_tasks()


def uninstall_tasks():
    """移除计划任务"""
    print("=" * 70)
    print("  开始卸载 A_DB Windows 计划任务")
    print("=" * 70)
    for t_name in [TASK_INTRADAY, TASK_POST_MARKET]:
        cmd = ["schtasks", "/Delete", "/TN", t_name, "/F"]
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode == 0:
            print(f"[成功] 计划任务已删除: {t_name}")
        else:
            print(f"[提示] 计划任务不存在或已删除: {t_name}")


def status_tasks():
    """查询计划任务状态"""
    print("=" * 70)
    print("  A_DB Windows 计划任务状态详情")
    print("=" * 70)
    for t_name in [TASK_INTRADAY, TASK_POST_MARKET]:
        print(f"\n>>> 任务名称: {t_name}")
        proc = subprocess.run(
            ["schtasks", "/Query", "/TN", t_name, "/FO", "LIST"],
            capture_output=True,
        )
        if proc.returncode == 0:
            out_str = proc.stdout.decode("gbk", errors="ignore")
            for line in out_str.splitlines():
                line_s = line.strip()
                if any(line_s.startswith(k) for k in [
                    "TaskName:", "任务名:",
                    "Next Run Time:", "下次运行时间:",
                    "Status:", "状态:",
                    "Logon Mode:", "登录模式:",
                    "Last Run Time:", "上次运行时间:",
                    "Last Result:", "上次结果:"
                ]):
                    print(f"  {line_s}")
        else:
            print("  [未注册] 任务未在系统中注册")


def main():
    parser = argparse.ArgumentParser(description="A_DB Windows 计划任务管理工具")
    parser.add_argument(
        "action",
        choices=["install", "uninstall", "status"],
        help="执行操作: install(注册安装), uninstall(卸载移除), status(状态查看)",
    )
    args = parser.parse_args()

    if args.action == "install":
        install_tasks()
    elif args.action == "uninstall":
        uninstall_tasks()
    elif args.action == "status":
        status_tasks()


if __name__ == "__main__":
    main()
